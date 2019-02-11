# coding=utf-8
"""
Entry point for producing WOfS products.

Specifically intended for running in the PBS job queue system at the NCI.

The three entry points are:
1. datacube-wofs submit
2. datacube-wofs generate
3. datacube-wofs run
"""
from __future__ import absolute_import, division
import logging
import os
import copy
from copy import deepcopy
from collections import defaultdict
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
from datacube.api.grid_workflow import Tile
from datacube.index import Index
from datacube.index.exceptions import MissingRecordError
from datacube.model import Range, DatasetType
from datacube.ui import click as ui
from datacube.ui import task_app
from datacube.utils import geometry
from datacube.utils.geometry import unary_union, unary_intersection, CRS
from datacube.drivers.netcdf import write_dataset_to_netcdf
from digitalearthau import serialise, paths
from digitalearthau.qsub import with_qsub_runner, QSubLauncher, TaskRunner
from digitalearthau.runners.model import TaskDescription
from digitalearthau.runners.util import init_task_app, submit_subjob
from wofs import wofls, __version__

APP_NAME = 'wofs'
_LOG = logging.getLogger(__name__)
_MEASUREMENT_KEYS_TO_COPY = ('zlib', 'complevel', 'shuffle', 'fletcher32', 'contiguous', 'attrs')

# ROOT_DIR is the current directory of this file.
ROOT_DIR = Path(__file__).absolute().parent.parent

INPUT_SOURCES = [{'nbart': 'ls5_nbart_albers',
                  'pq': 'ls5_pq_legacy_scene',
                  'sensor_name': 'TM',
                  'platform_name': 'LANDSAT-5',
                  'platform_name_short': 'ls5',
                  'source_product': 'ls5_level1_scene'},
                 {'nbart': 'ls7_nbart_albers',
                  'pq': 'ls7_pq_legacy_scene',
                  'sensor_name': 'ETM',
                  'platform_name': 'LANDSAT-7',
                  'platform_name_short': 'ls7',
                  'source_product': 'ls7_level1_scene'},
                 {'nbart': 'ls8_nbart_albers',
                  'pq': 'ls8_pq_legacy_scene',
                  'sensor_name': 'OLI',
                  'platform_name': 'LANDSAT-8',
                  'platform_name_short': 'ls8',
                  'source_product': 'ls8_level1_scene'},
                 ]


def polygon_from_sources_extents(sources, geobox):
    sources_union = geometry.unary_union(source.extent.to_crs(geobox.crs) for source in sources)
    valid_data = geobox.extent.intersection(sources_union)
    resolution = min([abs(x) for x in geobox.resolution])
    return valid_data.simplify(tolerance=resolution * 0.01)


def _make_wofs_config(index, config, dry_run):
    """
    Refine the configuration

    The task-app machinery loads a config file, from a path specified on the
    command line, into a dict. This function is an opportunity, with access to
    the datacube index, to modify that dict before it is used (being passed on
    to both the make-tasks and the do-task).

    For a dry run, we still need to create a dummy DatasetType to
    generate tasks (e.g. via the GridSpec), but a normal run must index
    it as a product in the database and replace the dummy with a fully-fleshed
    DatasetType since the tasks involve writing metadata to file that
    is specific to the database instance (note, this may change in future).
    """

    if not dry_run:
        _LOG.info('Created DatasetType %s', config['product_definition']['name'])  # true? not yet.

    config['wofs_dataset_type'] = _get_product(index, config['product_definition'], dry_run)

    config['variable_params'] = _build_variable_params(config)

    if 'task_timestamp' not in config:
        config['task_timestamp'] = int(time_now())

    if not os.access(config['location'], os.W_OK):
        _LOG.error('Current user appears not have write access output location: %s', config['location'])

    return config


def _build_variable_params(config: dict) -> dict:
    chunking = config['product_definition']['storage']['chunking']
    chunking = [chunking[dim] for dim in config['product_definition']['storage']['dimension_order']]

    variable_params = {}
    for mapping in config['product_definition']['measurements']:
        measurement_name = mapping['name']
        variable_params[measurement_name] = {
            k: v
            for k, v in mapping.items()
            if k in _MEASUREMENT_KEYS_TO_COPY
        }
        variable_params[measurement_name]['chunksizes'] = chunking
    return variable_params


def _create_output_definition(config: dict, source_product: DatasetType) -> dict:
    output_product_definition = deepcopy(source_product.definition)
    output_product_definition['name'] = config['product_definition']['name']
    output_product_definition['description'] = config['product_definition']['description']
    output_product_definition['managed'] = True
    var_def_keys = {'name', 'dtype', 'nodata', 'units', 'flags_definition'}

    output_product_definition['measurements'] = [
        {k: v for k, v in measurement.items() if k in var_def_keys}
        for measurement in config['product_definition']['measurements']
    ]

    output_product_definition['metadata']['format'] = {'name': 'NetCDF'}
    output_product_definition['metadata']['product_type'] = config.get('product_type', 'wofs')
    output_product_definition['metadata_type'] = config['product_definition']['metadata_type']

    output_product_definition['storage'] = {
        k: v for (k, v) in config['product_definition']['storage'].items()
        if k in ('crs', 'chunking', 'tile_size', 'resolution', 'dimension_order', 'driver')
    }

    # Validate the output product definition
    DatasetType.validate(output_product_definition)
    return output_product_definition


def _ensure_products(app_config: dict, index: Index, dry_run: bool, input_source) -> Tuple[DatasetType]:
    source_product_name = input_source
    source_product = index.products.get_by_name(source_product_name)
    if not source_product:
        raise ValueError(f"Source product {source_product_name} does not exist")

    output_product = DatasetType(
        source_product.metadata_type,
        _create_output_definition(app_config, source_product)
    )

    if not dry_run:
        _LOG.info('Add the output product definition for %s in the database.', output_product.name)
        output_product = index.products.add(output_product)

    return output_product


def _get_product(index, definition, dry_run):
    """
    Get the database record corresponding to the given product definition
    """
    metadata_type = index.metadata_types.get_by_name(definition['metadata_type'])
    prototype = DatasetType(metadata_type, definition)

    if not dry_run:
        _LOG.info('Add product definition to the database.')
        prototype = index.products.add(prototype)  # idempotent operation

    return prototype


def _get_filename(config, x, y, t):
    """
    Get file path from the config location
    """
    destination = config['location']
    filename_template = config['file_path_template']

    filename = filename_template.format(tile_index=(x, y),
                                        start_time=to_datetime(t).strftime('%Y%m%d%H%M%S%f'),
                                        version=config['task_timestamp'])  # A per JOB timestamp, seconds since epoch
    return Path(destination, filename)


def _group_tiles_by_cells(tile_index_list, cell_index_list):
    """
    Group the tiles by cells
    """
    key_map = defaultdict(list)
    for x, y, t in tile_index_list:
        if (x, y) in cell_index_list:
            key_map[(x, y)].append((x, y, t))
    return key_map


# pylint: disable=too-many-locals
def _generate_tasks(index, config, time, extent=None):
    """
    Yield tasks (loadables (nbart,ps,dsm) + output targets), for dispatch to workers.

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

    if dsm_loadables:
        _LOG.info('Found %d dsm loadables', len(dsm_loadables))
    else:
        _LOG.warning('No dsm1sv10 product in the database')

    for input_source in INPUT_SOURCES:
        gqa_filter = dict(product=input_source['source_product'], time=time, gqa_iterative_mean_xy=(0, 1))
        nbart_loadables = gw.list_tiles(product=input_source['nbart'], time=time, source_filter=gqa_filter, **extent)
        pq_loadables = gw.list_tiles(product=input_source['pq'], time=time, tile_buffer=pq_padding, **extent)

        _LOG.info('Found %d nbart loadables for %r input source', len(nbart_loadables), input_source['nbart'])
        _LOG.info('Found %d pq  loadables for %r input source', len(pq_loadables), input_source['pq'])

        # only valid where EO, PQ and DSM are *all* available (and WOFL isn't yet)
        tile_index_set = (set(nbart_loadables) & set(pq_loadables)) - set(wofls_loadables)
        key_map = _group_tiles_by_cells(tile_index_set, dsm_loadables)

        _LOG.info('Found %d items for %r input source', len(list(key_map.keys())), input_source['source_product'])

        # Cell index is X,Y, tile_index is X,Y,T
        for cell_index, tile_indexes in key_map.items():
            geobox = gw.grid_spec.tile_geobox(cell_index)
            dsm_tile = gw.update_tile_lineage(dsm_loadables[cell_index])
            for tile_index in tile_indexes:
                nbart_tile = gw.update_tile_lineage(nbart_loadables.pop(tile_index))
                pq_tile = gw.update_tile_lineage(pq_loadables.pop(tile_index))
                valid_region = _find_valid_data_region(geobox, nbart_tile, pq_tile, dsm_tile)
                if not valid_region.is_empty:
                    yield dict(source_tile=nbart_tile,
                               pq_tile=pq_tile,
                               dsm_tile=dsm_tile,
                               file_path=_get_filename(config, *tile_index),
                               tile_index=tile_index,
                               extra_global_attributes=dict(platform=input_source['platform_name'],
                                                            instrument=input_source['sensor_name']),
                               valid_region=valid_region)


def _make_wofs_tasks(index, config, year=None, **kwargs):
    """
    Generate an iterable of 'tasks', matching the provided filter parameters.
    Tasks can be generated for:
     - all of time
     - 1 particular year
     - a range of years
    Tasks can also be restricted to a given spatial region, specified in `kwargs['x']` and `kwargs['y']` in `EPSG:3577`.
    """
    # TODO: Filter query to valid options
    if isinstance(year, int):
        query_time = Range(datetime(year=year, month=1, day=1), datetime(year=year + 1, month=1, day=1))
    elif isinstance(year, tuple):
        query_time = Range(datetime(year=year[0], month=1, day=1), datetime(year=year[1] + 1, month=1, day=1))
    else:
        query_time = year

    extent = {}
    if 'x' in kwargs and kwargs['x']:
        extent['crs'] = 'EPSG:3577'
        extent['x'] = kwargs['x']
        extent['y'] = kwargs['y']

    tasks = _generate_tasks(index, config, time=query_time, extent=extent)
    return tasks


def _get_app_metadata(config):
    """
    Get WOfS app metadata
    """
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


def _find_valid_data_region(geobox, *sources_list):
    """
    Find the valid data region
    """
    # perform work in CRS of the output tile geobox
    unfused = [[dataset.extent.to_crs(geobox.crs) for dataset in tile.sources.item()]
               for tile in sources_list]
    # fuse the dataset extents within each source tile
    # pylint: disable=map-builtin-not-iterating
    tiles_extents = map(unary_union, unfused)
    # find where (within the output tile) that all prerequisite inputs available
    return unary_intersection([geobox.extent] + list(tiles_extents))
    # downstream should check if this is empty..


def _docvariable(agdc_dataset, time):
    """
    Convert datacube dataset to xarray/NetCDF variable
    """
    array = xarray.DataArray([agdc_dataset], coords=[time])
    docarray = datacube.model.utils.datasets_to_doc(array)
    docarray.attrs['units'] = '1'  # unitless (convention)
    return docarray


def _do_wofs_task(config, task):
    """
    Load data, run WOFS algorithm, attach metadata, and write output.
    :param dict config: Config object
    :param dict task: Dictionary of tasks

    :return: Dataset objects representing the generated data that can be added to the index
    :rtype: list(datacube.model.Dataset)
    """
    # datacube.api.Tile source_tile: NBAR Tile
    source_tile: Tile = task['source_tile']

    # datacube.api.Tile pq_tile: Pixel quality Tile
    pq_tile: Tile = task['pq_tile']

    # datacube.api.Tile dsm_tile: Digital Surface Model Tile
    dsm_tile: Tile = task['dsm_tile']

    # Path file_path: output file destination
    file_path = Path(task['file_path'])  # Path file_path: output file destination

    product = config['wofs_dataset_type']

    if file_path.exists():
        _LOG.warning('Output file already exists %r', str(file_path))

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
        valid_data=task['valid_region'],
        app_info=_get_app_metadata(config)
    )

    def harvest(what, tile):
        """
        Inherit optional metadata from EO, for future convenience only
        """
        datasets = [ds for source_datasets in tile.sources.values for ds in source_datasets]
        values = [dataset.metadata_doc[what] for dataset in datasets]
        assert all(value == values[0] for value in values)
        return copy.deepcopy(values[0])

    new_record.metadata_doc['platform'] = harvest('platform', source_tile)
    new_record.metadata_doc['instrument'] = harvest('instrument', source_tile)

    # copy metadata record into xarray
    result['dataset'] = _docvariable(new_record, result.time)

    global_attributes = config['global_attributes'].copy()
    global_attributes.update(task['extra_global_attributes'])

    # write output
    write_dataset_to_netcdf(result, file_path,
                            global_attributes=global_attributes,
                            variable_params=config['variable_params'])
    return [new_record]


def _index_datasets(index: Index, results):
    """
    Index newly created WOfS datasets
    """
    for dataset in results:
        try:
            index.datasets.add(dataset,
                               with_lineage=False,
                               sources_policy='skip')
            _LOG.info('Dataset %s added: %r', dataset.id, dataset)
        except (ValueError, MissingRecordError) as err:
            _LOG.error('Failed to add %r dataset: Error (%s)',
                       dataset,
                       err)


def _estimate_job_size(num_tasks):
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


# pylint: disable=invalid-name
tag_option = click.option('--tag', type=str,
                          default='notset',
                          help='Unique id for the job')

# pylint: disable=invalid-name
pbs_email_options = click.option('--email-options', '-m', default='abe',
                                 type=click.Choice(['a', 'b', 'e', 'n', 'ae', 'ab', 'be', 'abe']),
                                 help='Send Email when execution is, \n'
                                 '[a = aborted | b = begins | e = ends | n = do not send email]')

# pylint: disable=invalid-name
pbs_email_id = click.option('--email-id', '-M', default='nci.monitor@dea.ga.gov.au',
                            help='Email Recipient List')


@click.group(help='Datacube WOfS')
@click.version_option(version=__version__)
def cli():
    """
    Instantiate a click 'Datacube WOfS' group object to register the following sub-commands for
    different bits of WOfS processing:
         1) list
         2) ensure-products
         3) submit
         4) generate
         5) run
    :return: None
    """
    pass


@cli.command(name='list', help='List installed WOfS config files')
def list_configs():
    """
     List installed WOfS config files
    :return: None
    """
    # Since wofs config files are packaged two levels down the ROOT_DIR,
    # ROOT_DIR.parents[2] will ensure that we point to dea/<YYYYMMDD> directory.
    for cfg in ROOT_DIR.parents[2].glob('wofs/config/*.yaml'):
        click.echo(cfg)


@cli.command(name='ensure-products',
             help="Ensure the products exist for the given WOfS config, create them if necessary.")
@task_app.app_config_option
@click.option('--dry-run', is_flag=True, default=False,
              help='Check product definition without modifying the database')
@ui.config_option
@ui.verbose_option
@ui.pass_index(app_name=APP_NAME)
def ensure_products(index, app_config, dry_run):
    """
    Ensure the products exist for the given WOfS config, creating them if necessary.
    If dry run is disabled, the validated output product definition will be added to the database.
    """
    # TODO: Add more validation of config?
    click.secho(f"Loading {app_config}", bold=True)
    out_product = _ensure_products(paths.read_document(app_config),
                                   index,
                                   dry_run,
                                   'wofs_albers')
    click.secho(f"Output product definition for {out_product.name} product exits in the database for the given "
                f"WOfS input config file")


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
@pbs_email_options
@pbs_email_id
@click.option('--dry-run', is_flag=True, default=False, help='Check if output files already exist')
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
           tag: str,
           email_options: str,
           email_id: str,
           dry_run: bool):
    """
    Kick off two stage PBS job

    Stage 1 (Generate task file):
        The task-app machinery loads a config file, from a path specified on the
        command line, into a dict.

        If dry is enabled, a dummy DatasetType is created for tasks generation without indexing
        the product in the database.
        If dry run is disabled, generate tasks into file and queue PBS job to process them.

    Stage 2 (Run):
        During normal run, following are performed:
           1) Tasks (loadables (nbart,ps,dsm) + output targets) shall be yielded for dispatch to workers.
           2) Load data
           3) Run WOFS algorithm
           4) Attach metadata
           5) Write output files and
           6) Finally index the newly created WOfS output files

        If dry run is enabled, application only prepares a list of output files to be created and does not
        record anything in the database.
    """
    _LOG.info('Tag: %s', tag)

    app_config_path = Path(app_config).resolve()

    if not time_range or not all(time_range):
        query_args = Query(index=index).search_terms
    else:
        query_args = Query(index=index, time=time_range).search_terms

    task_desc, task_path = init_task_app(
        job_type="wofs",
        source_products=[],  # [app_config['source_product']],
        output_products=['wofs_albers'],
        # TODO: Use @datacube.ui.click.parsed_search_expressions to allow params other than time from the cli?
        datacube_query_args=query_args,
        app_config_path=app_config_path,
        pbs_project=project,
        pbs_queue=queue
    )
    _LOG.info("Created task description: %s", task_path)

    # If dry run is not enabled just pass verbose option
    dry_run_option = '--dry-run' if dry_run else '-v'
    extra_qsub_args = '-M {0} -m {1}'.format(email_id, email_options)

    # Append email options and email id to the PbsParameters dict key, extra_qsub_args
    task_desc.runtime_state.pbs_parameters.extra_qsub_args.extend(extra_qsub_args.split(' '))

    if no_qsub:
        _LOG.info('Skipping submission due to --no-qsub')
    else:
        submit_subjob(
            name='generate',
            task_desc=task_desc,
            command=[
                'generate', '-vv',
                '--task-desc', str(task_path),
                '--tag', tag,
                '--log-queries',
                '--email-id', email_id,
                '--email-options', email_options,
                dry_run_option,
            ],
            qsub_params=dict(
                name='wofs-generate-{}'.format(tag),
                mem='medium',
                wd=True,
                nodes=1,
                walltime='1h'))


@cli.command(help='Generate Tasks into file and Queue PBS job to process them')
@click.option('--no-qsub', is_flag=True, default=False, help="Skip submitting qsub for next step")
@click.option('--task-desc', 'task_desc_file', help='Task environment description file',
              required=True,
              type=click.Path(exists=True, readable=True, writable=False, dir_okay=False))
@tag_option
@pbs_email_options
@pbs_email_id
@click.option('--dry-run', is_flag=True, default=False, help='Check if output files already exist')
@ui.verbose_option
@ui.log_queries_option
@ui.pass_index(app_name=APP_NAME)
def generate(index: Index,
             task_desc_file: str,
             no_qsub: bool,
             tag: str,
             email_options: str,
             email_id: str,
             dry_run: bool):
    """
    Generate Tasks into file and Queue PBS job to process them

    If dry run is enabled, do not add the new products to the database.
    """
    _LOG.info('Tag: %s', tag)

    config, task_desc = _make_config_and_description(index, Path(task_desc_file), dry_run)

    num_tasks_saved = task_app.save_tasks(
        config,
        _make_wofs_tasks(index, config, year=task_desc.parameters.query.get('time')),
        str(task_desc.runtime_state.task_serialisation_path)
    )
    _LOG.info('Found %d tasks', num_tasks_saved)

    if not num_tasks_saved:
        _LOG.info("No tasks. Finishing.")
        return 0

    nodes, walltime = _estimate_job_size(num_tasks_saved)
    _LOG.info('Will request %d nodes and %s time', nodes, walltime)

    # If dry run is not enabled just pass verbose option
    dry_run_option = '--dry-run' if dry_run else '-v'
    extra_qsub_args = '-M {0} -m {1}'.format(email_id, email_options)

    # Append email options and email id to the PbsParameters dict key, extra_qsub_args
    task_desc.runtime_state.pbs_parameters.extra_qsub_args.extend(extra_qsub_args.split(' '))

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
            dry_run_option,
        ],
        qsub_params=dict(
            name='wofs-run-{}'.format(tag),
            mem='medium',
            wd=True,
            nodes=nodes,
            walltime=walltime
        ),
    )
    return 0


def _make_config_and_description(index: Index, task_desc_path: Path, dry_run: bool) -> Tuple[dict, TaskDescription]:
    task_desc = serialise.load_structure(task_desc_path, TaskDescription)

    task_time: datetime = task_desc.task_dt
    app_config = task_desc.runtime_state.config_path

    config = paths.read_document(app_config)

    # TODO: This carries over the old behaviour of each load. Should probably be replaced with *tag*
    config['task_timestamp'] = int(task_time.timestamp())
    config['app_config_file'] = Path(app_config)
    config = _make_wofs_config(index, config, dry_run)

    return config, task_desc


@cli.command(help='Process generated task file')
@click.option('--dry-run', is_flag=True, default=False, help='Check if output files already exist')
@click.option('--task-desc', 'task_desc_file', help='Task environment description file',
              required=True,
              type=click.Path(exists=True, readable=True, writable=False, dir_okay=False))
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
    """
    Process generated task file. If dry run is enabled, only check for the existing files
    """
    task_desc = serialise.load_structure(Path(task_desc_file), TaskDescription)
    config, tasks = task_app.load_tasks(task_desc.runtime_state.task_serialisation_path)

    if dry_run:
        _LOG.info('Starting WOfS Dry Run...')
        # tile_index is X, Y, T
        task_app.check_existing_files(_get_filename(config, *task['tile_index']) for task in tasks)
        return 0

    _LOG.info('Starting WOfS processing...')
    _LOG.info('Tag: %r', tag)
    task_func = partial(_do_wofs_task, config)
    process_func = partial(_index_datasets, index)

    try:
        runner(task_desc, tasks, task_func, process_func)
        _LOG.info("Runner finished normally, triggering shutdown.")
    finally:
        runner.stop()

    return 0


if __name__ == '__main__':
    cli()
