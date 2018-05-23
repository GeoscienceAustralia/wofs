# coding=utf-8
"""
Entry point for producing WOfS products.

Specifically intended for running in the PBS job queue system at the NCI.

The three entry points are:
1. datacube-wofs submit
2. datacube-wofs generate
3. datacube-wofs run
"""
import copy
import errno
import logging
import os
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from functools import partial
from math import ceil
from pathlib import Path
from time import time as time_now
from typing import Tuple

import numpy as np
import click
import xarray
from pandas import to_datetime

import datacube
import datacube.model.utils
from datacube.api.query import Query
from datacube.index import Index
from datacube.ui import click as ui

from datacube.compat import integer_types
from datacube.model import Range, DatasetType
from datacube.ui import task_app
from datacube.ui.task_app import check_existing_files
from datacube.utils.geometry import unary_union, unary_intersection, CRS
from digitalearthau import serialise, paths
from digitalearthau.qsub import with_qsub_runner, QSubLauncher, TaskRunner
from digitalearthau.runners.model import TaskDescription
from digitalearthau.runners.util import init_task_app, submit_subjob
from wofs import wofls, __version__

_LOG = logging.getLogger(__name__)
ROOT_DIR = Path(__file__).absolute().parent.parent
CONFIG_DIR = ROOT_DIR / 'config'
SCRIPT_DIR = ROOT_DIR / 'scripts'
_MEASUREMENT_KEYS_TO_COPY = ('zlib', 'complevel', 'shuffle', 'fletcher32', 'contiguous', 'attrs')

INPUT_SOURCES = [{'nbar': 'ls5_nbart_albers',
                  'pq': 'ls5_pq_legacy_scene',
                  'sensor_name': 'TM',
                  'platform_name': 'LANDSAT-5',
                  'platform_name_short': 'ls5',
                  'source_product': 'ls5_level1_scene'},
                 {'nbar': 'ls7_nbart_albers',
                  'pq': 'ls7_pq_legacy_scene',
                  'sensor_name': 'ETM',
                  'platform_name': 'LANDSAT-7',
                  'platform_name_short': 'ls7',
                  'source_product': 'ls7_level1_scene'},
                 {'nbar': 'ls8_nbart_albers',
                  'pq': 'ls8_pq_legacy_scene',
                  'sensor_name': 'OLI',
                  'platform_name': 'LANDSAT-8',
                  'platform_name_short': 'ls8',
                  'source_product': 'ls8_level1_scene'},
                 ]


def make_wofs_config(index, config, dry_run=False, **query):
    """
    Refine the configuration

    The task-app machinery loads a config file, from a path specified on the
    command line, into a dict. This function is an opportunity, with access to
    the datacube index, to modify that dict before it is used (being passed on
    to both the make-tasks and the do-task). If using the save-tasks option,
    the modified config is included in the task file.

    For a dry run, we still need to create a dummy DatasetType to
    generate tasks (e.g. via the GridSpec), but a normal run must index
    it as a product in the database and replace the dummy with a fully-fleshed
    DatasetType since the tasks involve writing metadata to file that
    is specific to the database instance (note, this may change in future).
    """

    if not os.access(config['location'], os.W_OK):
        _LOG.warning('Current user appears not have write access output location: %s', config['location'])

    config['wofs_dataset_type'] = get_product(index, config['product_definition'])

    config['variable_params'] = _build_variable_params(config)

    if 'task_timestamp' not in config:
        config['task_timestamp'] = int(time_now())

    return config


def _build_variable_params(config: dict) -> dict:
    chunking = config['product_definition']['storage']['chunking']
    chunking = [chunking[dim] for dim in config['product_definition']['storage']['dimension_order']]

    variable_params = {}
    for mapping in config['product_definition']['measurements']:
        measurment_name = mapping['name']
        variable_params[measurment_name] = {
            k: v
            for k, v in mapping.items()
            if k in _MEASUREMENT_KEYS_TO_COPY
        }
        variable_params[measurment_name]['chunksizes'] = chunking
    return variable_params


def _create_output_definition(config: dict, source_product: DatasetType) -> dict:
    output_product_definition = deepcopy(source_product.definition)
    output_product_definition['name'] = config['output_product']
    output_product_definition['managed'] = True
    output_product_definition['description'] = config['description']
    output_product_definition['metadata']['format'] = {'name': 'NetCDF'}
    output_product_definition['metadata']['product_type'] = config.get('product_type', 'fractional_cover')
    output_product_definition['storage'] = {
        k: v for (k, v) in config['storage'].items()
        if k in ('crs', 'tile_size', 'resolution', 'origin')
    }
    var_def_keys = {'name', 'dtype', 'nodata', 'units', 'aliases', 'spectral_definition', 'flags_definition'}

    output_product_definition['measurements'] = [
        {k: v for k, v in measurement.items() if k in var_def_keys}
        for measurement in config['measurements']
    ]
    return output_product_definition


def get_product(index, definition, dry_run=False, skip_indexing=False):
    """
    Get the database record corresponding to the given product definition
    """
    metadata_type = index.metadata_types.get_by_name(definition['metadata_type'])
    prototype = datacube.model.DatasetType(metadata_type, definition)

    if not dry_run and not skip_indexing:
        prototype = index.products.add(prototype)  # idempotent operation

    return prototype


def get_filename(config, x, y, t):
    destination = config['location']
    filename_template = config['file_path_template']

    filename = filename_template.format(tile_index=(x, y),
                                        start_time=to_datetime(t).strftime('%Y%m%d%H%M%S%f'),
                                        version=config['task_timestamp'])  # A per JOB timestamp, seconds since epoch
    return Path(destination, filename)


def group_tiles_by_cells(tile_index_list, cell_index_list):
    key_map = defaultdict(list)
    for x, y, t in tile_index_list:
        if (x, y) in cell_index_list:
            key_map[(x, y)].append((x, y, t))
    return key_map


def generate_tasks(index, config, time, extent=None):
    """
    Yield tasks (loadables (nbar,ps,dsm) + output targets), for dispatch to workers.

    This function is the equivalent of an SQL join query,
    and is required as a workaround for datacube API abstraction layering.
    """
    extent = extent if extent is not None else {}
    product = config['wofs_dataset_type']

    assert product.grid_spec.crs == CRS('EPSG:3577')
    assert all((abs(r) == 25) for r in product.grid_spec.resolution)  # ensure approx. 25 metre raster
    pq_padding = [3 * 25] * 2  # for 3 pixel cloud dilation
    terrain_padding = [6850] * 2
    # Worst case shadow: max prominence (Kosciuszko) at lowest solar declination (min incidence minus slope threshold)
    # with snapping to pixel edges to avoid API questions
    # e.g. 2230 metres / math.tan(math.radians(30-12)) // 25 * 25 == 6850

    gw = datacube.api.GridWorkflow(index, grid_spec=product.grid_spec)  # GridSpec from product definition

    wofls_loadables = gw.list_tiles(product=product.name, time=time, **extent)
    dsm_loadables = gw.list_cells(product='dsm1sv10', tile_buffer=terrain_padding, **extent)

    for input_source in INPUT_SOURCES:
        gqa_filter = dict(product=input_source['source_product'], time=time, gqa_iterative_mean_xy=(0, 1))
        nbar_loadables = gw.list_tiles(product=input_source['nbar'], time=time, source_filter=gqa_filter, **extent)
        pq_loadables = gw.list_tiles(product=input_source['pq'], time=time, tile_buffer=pq_padding, **extent)

        # only valid where EO, PQ and DSM are *all* available (and WOFL isn't yet)
        tile_index_set = (set(nbar_loadables) & set(pq_loadables)) - set(wofls_loadables)
        key_map = group_tiles_by_cells(tile_index_set, dsm_loadables)

        # Cell index is X,Y, tile_index is X,Y,T
        for cell_index, tile_indexes in key_map.items():
            geobox = gw.grid_spec.tile_geobox(cell_index)
            dsm_tile = gw.update_tile_lineage(dsm_loadables[cell_index])
            for tile_index in tile_indexes:
                nbar_tile = gw.update_tile_lineage(nbar_loadables.pop(tile_index))
                pq_tile = gw.update_tile_lineage(pq_loadables.pop(tile_index))
                valid_region = find_valid_data_region(geobox, nbar_tile, pq_tile, dsm_tile)
                if not valid_region.is_empty:
                    yield dict(source_tile=nbar_tile,
                               pq_tile=pq_tile,
                               dsm_tile=dsm_tile,
                               file_path=get_filename(config, *tile_index),
                               tile_index=tile_index,
                               extra_global_attributes=dict(platform=input_source['platform_name'],
                                                            instrument=input_source['sensor_name']),
                               valid_region=valid_region)


def make_wofs_tasks(index, config, query, **kwargs):
    """
    Generate an iterable of 'tasks', matching the provided filter parameters.

    Tasks can be generated for:

     - all of time
     - 1 particular year
     - a range of years

    Tasks can also be restricted to a given spatial region, specified in `kwargs['x']` and `kwargs['y']` in `EPSG:3577`.
    """
    # TODO: Filter query to valid options

    time = query.get('time')

    extent = {}
    if 'x' in query and query['x']:
        extent['crs'] = 'EPSG:3577'
        extent['x'] = query['x']
        extent['y'] = query['y']

    tasks = generate_tasks(index, config, time=time, extent=extent)
    return tasks


def get_app_metadata(config):
    doc = {
        'lineage': {
            'algorithm': {
                'name': APP_NAME,
                'version': __version__,
                'repo_url': 'https://github.com/GeoscienceAustralia/wofs.git',
                'parameters': {'configuration_file': str(config['app_config_file'])}
            },
        }
    }
    return doc


def find_valid_data_region(geobox, *sources_list):
    # perform work in CRS of the output tile geobox
    unfused = [[dataset.extent.to_crs(geobox.crs) for dataset in tile.sources.item()]
               for tile in sources_list]
    # fuse the dataset extents within each source tile
    tiles_extents = map(unary_union, unfused)
    # find where (within the output tile) that all prerequisite inputs available
    return unary_intersection([geobox.extent] + list(tiles_extents))
    # downstream should check if this is empty..


def docvariable(agdc_dataset, time):
    """
    Convert datacube dataset to xarray/NetCDF variable
    """
    array = xarray.DataArray([agdc_dataset], coords=[time])
    docarray = datacube.model.utils.datasets_to_doc(array)
    docarray.attrs['units'] = '1'  # unitless (convention)
    return docarray


def do_wofs_task(config, source_tile, pq_tile, dsm_tile, file_path, tile_index, extra_global_attributes, valid_region):
    """
    Load data, run WOFS algorithm, attach metadata, and write output.

    :param dict config: Config object
    :param datacube.api.Tile source_tile: NBAR Tile
    :param datacube.api.Tile pq_tile: Pixel quality Tile
    :param datacube.api.Tile dsm_tile: Digital Surface Model Tile
    :param Path file_path: output file destination
    :param tuple tile_index: Index of the tile

    :return: Dataset objects representing the generated data that can be added to the index
    :rtype: list(datacube.model.Dataset)
    """
    product = config['wofs_dataset_type']
    app_info = get_app_metadata(config)

    if file_path.exists():
        raise OSError(errno.EEXIST, 'Output file already exists', str(file_path))

    # load data
    bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']  # inputs needed from EO data)
    source = datacube.api.GridWorkflow.load(source_tile, measurements=bands)
    pq = datacube.api.GridWorkflow.load(pq_tile)
    dsm = datacube.api.GridWorkflow.load(dsm_tile, resampling='cubic')

    # Core computation
    result = wofls.woffles(*(x.isel(time=0) for x in [source, pq, dsm])).astype(np.int16)

    # Convert 2D DataArray to 3D DataSet
    result = xarray.concat([result], dim=source.time).to_dataset(name='water')

    # add metadata
    result.water.attrs['nodata'] = 1  # lest it default to zero (i.e. clear dry)
    result.water.attrs['units'] = '1'  # unitless (convention)
    result.water.attrs['crs'] = source.crs

    # Attach CRS. Note this is poorly represented in NetCDF-CF
    # (and unrecognised in xarray), likely improved by datacube-API model.
    result.attrs['crs'] = source.crs

    # Provenance tracking
    parent_sources = [ds for tile in [source_tile, pq_tile, dsm_tile] for ds in tile.sources.values[0]]

    # Create indexable record
    new_record = datacube.model.utils.make_dataset(
        product=product,
        sources=parent_sources,
        center_time=result.time.values[0],
        uri=file_path.absolute().as_uri(),
        extent=source_tile.geobox.extent,
        valid_data=valid_region,
        app_info=app_info
    )

    # inherit optional metadata from EO, for future convenience only
    def harvest(what, tile):
        datasets = [ds for source_datasets in tile.sources.values for ds in source_datasets]
        values = [dataset.metadata_doc[what] for dataset in datasets]
        assert all(value == values[0] for value in values)
        return copy.deepcopy(values[0])

    new_record.metadata_doc['platform'] = harvest('platform', source_tile)
    new_record.metadata_doc['instrument'] = harvest('instrument', source_tile)

    # copy metadata record into xarray
    result['dataset'] = docvariable(new_record, result.time)

    global_attributes = config['global_attributes'].copy()
    global_attributes.update(extra_global_attributes)

    # write output
    datacube.storage.storage.write_dataset_to_netcdf(result, file_path,
                                                     global_attributes=global_attributes,
                                                     variable_params=config['variable_params'])
    return [new_record]


def process_result(index: Index, result):
    for dataset in result.values:
        index.datasets.add(dataset, sources_policy='skip')
        _LOG.info('Dataset %s added at %s', dataset.id, dataset.uris)


APP_NAME = 'wofs'


# pylint: disable=invalid-name
tag_option = click.option('--tag', type=str,
                          default='notset',
                          help='Unique id for the job')


@click.group(help='Datacube WOfS')
@click.version_option(version=__version__)
def cli():
    pass


@cli.command(name='list', help='List installed WOfS config files')
def list_configs():
    for cfg in CONFIG_DIR.glob('*.yaml'):
        click.echo(cfg)


@cli.command(
    help="Ensure the products exist for the given WOfS config, creating them if necessary."
)
@click.argument(
    'app-config-files',
    nargs=-1,
    type=click.Path(exists=True, readable=True, writable=False, dir_okay=False)
)
@ui.config_option
@ui.verbose_option
@ui.pass_index(app_name=APP_NAME)
def ensure_products(index, app_config_files):
    for app_config_file in app_config_files:
        # TODO: Add more validation of config?

        click.secho(f"Loading {app_config_file}", bold=True)
        app_config = paths.read_document(app_config_file)


def estimate_job_size(num_tasks):
    """ Translate num_tasks to number of nodes and walltime
    """
    max_nodes = 20
    cores_per_node = 16
    task_time_mins = 5

    # TODO: Tune this code:
    # "We have found for best throughput 25 nodes can produce about 11.5 tiles per minute per node,
    # with a CPU efficiency of about 96%."
    if num_tasks < max_nodes * cores_per_node:
        nodes = ceil(num_tasks / cores_per_node / 4)  # If fewer tasks than max cores, try to get 4 tasks to a core
    else:
        nodes = max_nodes

    tasks_per_cpu = ceil(num_tasks / (nodes * cores_per_node))
    wall_time_mins = '{mins}m'.format(mins=(task_time_mins * tasks_per_cpu))

    return nodes, wall_time_mins


@cli.command(help='Kick off two stage PBS job')
@click.option('--project', '-P', default='u46')
@click.option('--queue', '-q', default='normal',
              type=click.Choice(['normal', 'express']))
@click.option('--year', 'time_range',
              callback=task_app.validate_year,
              help='Limit the process to a particular year')
@click.option('--no-qsub', is_flag=True, default=False,
              help="Skip submitting job")
@tag_option
@task_app.app_config_option
@ui.config_option
@ui.verbose_option
@ui.pass_index(app_name=APP_NAME)
def submit(index: Index,
           app_config: str,
           project: str,
           queue: str,
           no_qsub: bool,
           time_range: Tuple[datetime, datetime],
           tag: str):
    _LOG.info('Tag: %s', tag)

    app_config_path = Path(app_config).resolve()
    app_config = paths.read_document(app_config_path)

    task_desc, task_path = init_task_app(
        job_type="wofs",
        source_products=[],  # [app_config['source_product']],
        output_products=['wofs_albers'],
        # TODO: Use @datacube.ui.click.parsed_search_expressions to allow params other than time from the cli?
        datacube_query_args=Query(index=index, time=time_range).search_terms,
        app_config_path=app_config_path,
        pbs_project=project,
        pbs_queue=queue
    )
    _LOG.info("Created task description: %s", task_path)

    if no_qsub:
        _LOG.info('Skipping submission due to --no-qsub')
        return 0

    submit_subjob(
        name='generate',
        task_desc=task_desc,
        command=[
            'generate', '-v', '-v',
            '--task-desc', str(task_path),
            '--tag', tag
        ],
        qsub_params=dict(
            mem='20G',
            wd=True,
            ncpus=1,
            walltime='1h',
            name='wofs-generate-{}'.format(tag)
        )
    )


@cli.command(help='Generate Tasks into file and Queue PBS job to process them')
@click.option('--no-qsub', is_flag=True, default=False, help="Skip submitting qsub for next step")
@click.option(
    '--task-desc', 'task_desc_file', help='Task environment description file',
    required=True,
    type=click.Path(exists=True, readable=True, writable=False, dir_okay=False)
)
@tag_option
@ui.verbose_option
@ui.log_queries_option
@ui.pass_index(app_name=APP_NAME)
def generate(index: Index,
             task_desc_file: str,
             no_qsub: bool,
             tag: str):
    _LOG.info('Tag: %s', tag)

    config, task_desc = _make_config_and_description(index, Path(task_desc_file))

    num_tasks_saved = task_app.save_tasks(
        config,
        make_wofs_tasks(index, config, query=task_desc.parameters.query),
        str(task_desc.runtime_state.task_serialisation_path)
    )
    _LOG.info('Found %d tasks', num_tasks_saved)

    if not num_tasks_saved:
        _LOG.info("No tasks. Finishing.")
        return 0

    nodes, walltime = estimate_job_size(num_tasks_saved)
    _LOG.info('Will request %d nodes and %s time', nodes, walltime)

    if no_qsub:
        _LOG.info('Skipping submission due to --no-qsub')
        return 0

    submit_subjob(
        name='run',
        task_desc=task_desc,

        command=[
            'run',
            '-vv',
            '--task-desc', str(task_desc_file),
            '--celery', 'pbs-launch',
            '--tag', tag,
        ],
        qsub_params=dict(
            name='wofs-run-{}'.format(tag),
            mem='small',
            wd=True,
            nodes=nodes,
            walltime=walltime
        ),
    )


def _make_config_and_description(index: Index, task_desc_path: Path) -> Tuple[dict, TaskDescription]:
    task_desc = serialise.load_structure(task_desc_path, TaskDescription)

    task_time: datetime = task_desc.task_dt
    app_config = task_desc.runtime_state.config_path

    config = paths.read_document(app_config)

    # TODO: This carries over the old behaviour of each load. Should probably be replaced with *tag*
    config['task_timestamp'] = int(task_time.timestamp())
    config['app_config_file'] = Path(app_config)
    config = make_wofs_config(index, config)

    return config, task_desc


@cli.command(help='Actually process generated task file')
@click.option('--dry-run', is_flag=True, default=False, help='Check if output files already exist')
@click.option(
    '--task-desc', 'task_desc_file', help='Task environment description file',
    required=True,
    type=click.Path(exists=True, readable=True, writable=False, dir_okay=False)
)
@with_qsub_runner()
@task_app.load_tasks_option
@tag_option
@ui.config_option
@ui.verbose_option
@ui.pass_index(app_name=APP_NAME)
def run(index,
        dry_run: bool,
        tag: str,
        task_desc_file: str,
        qsub: QSubLauncher,
        runner: TaskRunner,
        *args, **kwargs):
    _LOG.info('Starting WOfS processing...')
    _LOG.info('Tag: %r', tag)

    task_desc = serialise.load_structure(Path(task_desc_file), TaskDescription)
    config, tasks = task_app.load_tasks(task_desc.runtime_state.task_serialisation_path)

    if dry_run:
        task_app.check_existing_files((task['filename'] for task in tasks))
        return 0

    task_func = partial(do_wofs_task, config)
    process_func = partial(process_result, index)

    try:
        runner(task_desc, tasks, task_func, process_func)
        _LOG.info("Runner finished normally, triggering shutdown.")
    finally:
        runner.stop()


if __name__ == '__main__':
    cli()
