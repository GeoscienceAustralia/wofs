"""
Copied from the NDVI/FC template, this is a quick and dirty approach to
producing a datacube application with full compatibility.
"""

from __future__ import absolute_import, print_function

import errno
import itertools
import logging
import os
from datetime import datetime
from collections import defaultdict
import functools

import click
from pandas import to_datetime
from pathlib import Path
import xarray

import datacube
from datacube.compat import integer_types
from datacube.model import DatasetType, GeoPolygon, Range
import datacube.model.utils
from datacube.storage.storage import write_dataset_to_netcdf
from datacube.ui import click as ui
from datacube.ui.task_app import task_app, task_app_options, check_existing_files

from . import wofls


_LOG = logging.getLogger('agdc-wofs')

SENSORS = {'ls8': 'LS8_OLI', 'ls7': 'LS7_ETM', 'ls5': 'LS5_TM'}  # { nbar-prefix : filename-prefix } for platforms


def get_product(index, definition, dry_run=False):
    """Utility to get database-record corresponding to product-definition"""
    parsed = definition
    metadata_type = index.metadata_types.get_by_name(parsed['metadata_type'])
    prototype = datacube.model.DatasetType(metadata_type, parsed)
    return prototype if dry_run else index.products.add(prototype)  # add is idempotent


def make_wofs_config(index, config, dry_run=False, **query):
    """ Refine the configuration

    The task-app machinery loads a config file, from a path specified on the
    command line, into a dict. This function is an opportunity, with access to
    the datacube index, to modify that dict before it is used (being passed on
    to both the make-tasks and the do-task). If using the save-tasks option,
    the modified config is included in the task file.

    For a dry run, still needs to create a dummy DatasetType for use to
    generate tasks (e.g. via the GridSpec), but for a normal run must index
    it as a product in the database and replace with a fully-fleshed out
    DatasetType object as the tasks involve writing metadata to file that
    is specific to the database instance (note, this may change in future).
    """

    if not dry_run:
        _LOG.info('Created DatasetType %s', config['product_definition']['name'])  # true? nyet.

    config['wofs_dataset_type'] = get_product(index, config['product_definition'], dry_run)

    if not os.access(config['location'], os.W_OK):
        _LOG.warn('Current user appears not have write access output location: %s', config['location'])

    return config


def get_filename(config, sensor, x, y, t):
    destination = config['location']
    filename_template = config['file_path_template']

    filename = filename_template.format(sensor=sensor,
                                        tile_index=(x, y),
                                        time=to_datetime(t).strftime('%Y%m%d%H%M%S%f'))
    return Path(destination, filename)


def group_tiles_by_cells(tile_index_list, cell_index_list):
    key_map = defaultdict(list)
    for x, y, t in tile_index_list:
        if (x, y) in cell_index_list:
            key_map[(x, y)].append((x, y, t))
    return key_map


def generate_tasks(index, config, time):
    """ Yield loadables (nbar,ps,dsm) and targets, for dispatch to workers.

    This function is the equivalent of an SQL join query,
    and is required as a workaround for datacube API abstraction layering.
    """
    extent = {}  # not configurable
    product = config['wofs_dataset_type']

    gw = datacube.api.GridWorkflow(index, grid_spec=product.grid_spec)  # GridSpec from product definition

    wofls_loadables = gw.list_tiles(product=product.name, time=time, **extent)
    dsm_loadables = gw.list_cells(product='dsm1sv10', **extent)

    for platform, sensor in SENSORS.items():
        source_loadables = gw.list_tiles(product=platform+'_nbar_albers', time=time, **extent)
        pq_loadables = gw.list_tiles(product=platform+'_pq_albers', time=time, **extent)

        # only valid where EO, PQ and DSM are *all* available (and WOFL isn't yet)
        tile_index_set = (set(source_loadables) & set(pq_loadables)) - set(wofls_loadables)
        key_map = group_tiles_by_cells(tile_index_set, dsm_loadables)

        for dsm_key, keys in key_map.items():
            dsm_tile = gw.update_tile_lineage(dsm_loadables[dsm_key])
            for tile_index in keys:
                source_tile = gw.update_tile_lineage(source_loadables[tile_index])
                pq_tile = gw.update_tile_lineage(pq_loadables[tile_index])
                yield ((source_tile, pq_tile, dsm_tile), get_filename(config, sensor, *tile_index))


def make_wofs_tasks(index, config, year=None, **kwargs):
    # TODO: Filter query to valid options
    time = None
    if year is not None:
        if isinstance(year, integer_types):
            time = Range(datetime(year=year, month=1, day=1), datetime(year=year+1, month=1, day=1))
        elif isinstance(year, tuple):
            time = Range(datetime(year=year[0], month=1, day=1), datetime(year=year[1]+1, month=1, day=1))

    tasks = generate_tasks(index, config, time=time)
    return tasks


def get_app_metadata(config):
    doc = {
        'lineage': {
            'algorithm': {
                'name': 'datacube-wofs',
                'version': config.get('version', 'unknown'),
                'repo_url': 'https://github.com/GeoscienceAustralia/wofs.git',
                'parameters': {'configuration_file': config.get('app_config_file', 'unknown')}
            },
        }
    }
    return doc


def find_valid_data_region(geobox, *sources_list):
    footprints = [datacube.utils.union_points(*[source.extent.to_crs(geobox.crs).points for source in sources])
                  for sources in sources_list]
    # TODO: Remove reduce when intersect_points that supports multiple args becomes availible
    valid_data = functools.reduce(datacube.utils.intersect_points, [geobox.extent.points] + footprints)

    return GeoPolygon(valid_data, geobox.crs)


def docvariable(agdc_dataset, time):
    """Convert datacube dataset to xarray/NetCDF variable"""
    array = xarray.DataArray([agdc_dataset], coords=[time])
    docarray = datacube.model.utils.datasets_to_doc(array)
    docarray.attrs['units'] = '1' # unitless (convention)
    return docarray


def do_wofs_task(config, (loadables, file_path)):
    """ Load data, run WOFS algorithm, attach metadata, and write output.

    Input:
        - three-tuple of Tile objects (NBAR, PQ, DSM)
        - path object (output file destination)
    Output:
        - indexable object (referencing output data location)
    """
    product = config['wofs_dataset_type']
    app_info = get_app_metadata(config)

    if file_path.exists():
        raise OSError(errno.EEXIST, 'Output file already exists', str(file_path))

    # load data
    source_tile, pq_tile, dsm_tile = loadables
    bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']  # inputs needed from EO data)
    source = datacube.api.GridWorkflow.load(source_tile, measurements=bands)
    pq = datacube.api.GridWorkflow.load(pq_tile)
    dsm = datacube.api.GridWorkflow.load(dsm_tile, resampling='cubic')

    # Core computation
    result = wofls.woffles(*(x.isel(time=0) for x in [source, pq, dsm]))

    # Convert 2D DataArray to 3D DataSet
    result = xarray.concat([result], dim=source.time).to_dataset(name='water')

    # add metadata
    result.water.attrs['nodata'] = 1  # lest it default to zero (i.e. clear dry)
    result.water.attrs['units'] = '1'  # unitless (convention)

    # Attach CRS. Note this is poorly represented in NetCDF-CF
    # (and unrecognised in xarray), likely improved by datacube-API model.
    result.attrs['crs'] = source.crs

    # Provenance tracking
    parent_sources = [ds for tile in loadables for ds in tile.sources.values[0]]

    # Create indexable record
    new_record = datacube.model.utils.make_dataset(
        product=product,
        sources=parent_sources,
        center_time=result.time.values[0],
        uri=file_path.absolute().as_uri(),
        extent=source_tile.geobox.extent,
        valid_data=find_valid_data_region(result.water.geobox, source_tile, pq_tile, dsm_tile),
        app_info=app_info
    )

    # inherit optional metadata from EO, for future convenience only
    def harvest(what, tile):
        datasets = [ds for source_datasets in tile.sources.values for ds in source_datasets]
        values = [dataset.metadata_doc[what] for dataset in datasets]
        assert all(value == values[0] for value in values)
        return values[0]

    new_record.metadata_doc['platform'] = harvest('platform', source_tile)
    new_record.metadata_doc['instrument'] = harvest('instrument', source_tile)

    # copy metadata record into xarray
    result['dataset'] = docvariable(new_record, result.time)

    # write output
    datacube.storage.storage.write_dataset_to_netcdf(result, file_path,
                                                     global_attributes=config['global_attributes'],
                                                     variable_params=config['variable_params'])

    return [new_record]


def validate_year(ctx, param, value):
    try:
        if value is None:
            return None
        years = map(int, value.split('-', 2))
        if len(years) == 1:
            return years[0]
        return tuple(years)
    except ValueError:
        raise click.BadParameter('year must be specified as a single year (eg 1996) '
                                 'or as an inclusive range (eg 1996-2001)')


APP_NAME = 'wofs'


@click.command(name=APP_NAME)
@ui.pass_index(app_name=APP_NAME)
@click.option('--dry-run', is_flag=True, default=False, help='Check if output files already exist')
@click.option('--year', callback=validate_year, help='Limit the process to a particular year')
@click.option('--queue-size', '--backlog', type=click.IntRange(1, 100000), default=3200,
              help='Number of tasks to queue at the start')
@task_app_options
@task_app(make_config=make_wofs_config, make_tasks=make_wofs_tasks)
def wofs_app(index, config, tasks, executor, dry_run, queue_size, *args, **kwargs):
    click.echo('Starting processing...')

    if dry_run:
        check_existing_files((task['filename'] for task in tasks))
        return 0

    results = []

    def submit_task(task):
        _LOG.info('Queuing task: %s', task['tile_index'])
        results.append(executor.submit(do_wofs_task, config=config, task=task))

    task_queue = itertools.islice(tasks, queue_size)
    for task in task_queue:
        submit_task(task)
    click.echo('Queue filled, waiting for first result...')

    successful = failed = 0
    while results:
        result, results = executor.next_completed(results, None)

        # submit a new task to replace the one we just finished
        task = next(tasks, None)
        if task:
            submit_task(task)

        # Process the result
        try:
            datasets = executor.result(result)
            for dataset in datasets.values:
                index.datasets.add(dataset, skip_sources=True)
                _LOG.info('Dataset added')
            successful += 1
        except Exception as err:  # pylint: disable=broad-except
            _LOG.exception('Task failed: %s', err)
            failed += 1
            continue
        finally:
            # Release the task to free memory so there is no leak in executor/scheduler/worker process
            executor.release(result)

    click.echo('%d successful, %d failed' % (successful, failed))
    _LOG.info('Completed: %d successful, %d failed', successful, failed)


if __name__ == '__main__':
    wofs_app()
