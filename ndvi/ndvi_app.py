from __future__ import absolute_import, print_function

import errno
import itertools
import logging
import os
from copy import deepcopy
from datetime import datetime

import click
from pandas import to_datetime
from pathlib import Path
import xarray
import numpy as np

from datacube.api.grid_workflow import GridWorkflow
from datacube.compat import integer_types
from datacube.model import DatasetType, GeoPolygon, Range
from datacube.model.utils import make_dataset, xr_apply, datasets_to_doc
from datacube.storage.storage import write_dataset_to_netcdf
from datacube.storage.masking import mask_valid_data
from datacube.ui import click as ui
from datacube.ui.task_app import task_app, task_app_options, check_existing_files
from datacube.utils import intersect_points, union_points


_LOG = logging.getLogger('agdc-ndvi')


def make_ndvi_config(index, config, dry_run=False, **query):
    source_type = index.products.get_by_name(config['source_type'])
    if not source_type:
        _LOG.error("Source DatasetType %s does not exist", config['source_type'])
        return 1

    output_type_definition = deepcopy(source_type.definition)
    output_type_definition['name'] = config['output_type']
    output_type_definition['managed'] = True
    output_type_definition['description'] = config['description']
    output_type_definition['storage'] = config['storage']
    output_type_definition['metadata']['format'] = {'name': 'NetCDF'}
    output_type_definition['metadata']['product_type'] = config.get('product_type', 'ndvi')

    var_def_keys = {'name', 'dtype', 'nodata', 'units', 'aliases', 'spectral_definition', 'flags_definition'}

    output_type_definition['measurements'] = [{k: v for k, v in measurement.items() if k in var_def_keys}
                                              for measurement in config['measurements']]

    chunking = config['storage']['chunking']
    chunking = [chunking[dim] for dim in config['storage']['dimension_order']]

    var_param_keys = {'zlib', 'complevel', 'shuffle', 'fletcher32', 'contiguous', 'attrs'}
    variable_params = {}
    for mapping in config['measurements']:
        varname = mapping['name']
        variable_params[varname] = {k: v for k, v in mapping.items() if k in var_param_keys}
        variable_params[varname]['chunksizes'] = chunking

    config['variable_params'] = variable_params

    output_type = DatasetType(source_type.metadata_type, output_type_definition)

    if not dry_run:
        _LOG.info('Created DatasetType %s', output_type.name)
        output_type = index.products.add(output_type)

    if not os.access(config['location'], os.W_OK):
        _LOG.warn('Current user appears not have write access output location: %s', config['location'])

    config['nbar_dataset_type'] = source_type
    config['ndvi_dataset_type'] = output_type

    return config


def get_filename(config, tile_index, sources):
    file_path_template = str(Path(config['location'], config['file_path_template']))
    return file_path_template.format(tile_index=tile_index,
                                     start_time=to_datetime(sources.time.values[0]).strftime('%Y%m%d%H%M%S%f'),
                                     end_time=to_datetime(sources.time.values[-1]).strftime('%Y%m%d%H%M%S%f'))


def make_ndvi_tasks(index, config, year=None, **kwargs):
    input_type = config['nbar_dataset_type']
    output_type = config['ndvi_dataset_type']

    workflow = GridWorkflow(index, output_type.grid_spec)

    # TODO: Filter query to valid options
    query = {}
    if year is not None:
        if isinstance(year, integer_types):
            query['time'] = Range(datetime(year=year, month=1, day=1), datetime(year=year+1, month=1, day=1))
        elif isinstance(year, tuple):
            query['time'] = Range(datetime(year=year[0], month=1, day=1), datetime(year=year[1]+1, month=1, day=1))

    tiles_in = workflow.list_tiles(product=input_type.name, **query)
    tiles_out = workflow.list_tiles(product=output_type.name, **query)

    def make_task(tile, **task_kwargs):
        task = dict(nbar=workflow.update_tile_lineage(tile))
        task.update(task_kwargs)
        return task

    tasks = [make_task(tile, tile_index=key, filename=get_filename(config, tile_index=key, sources=tile.sources))
             for key, tile in tiles_in.items() if key not in tiles_out]

    _LOG.info('%s tasks discovered', len(tasks))
    return tasks


def get_app_metadata(config):
    doc = {
        'lineage': {
            'algorithm': {
                'name': 'datacube-ndvi',
                'version': config.get('version', 'unknown'),
                'repo_url': 'https://github.com/GeoscienceAustralia/ndvi.git',
                'parameters': {'configuration_file': config.get('app_config_file', 'unknown')}
            },
        }
    }
    return doc


def calculate_ndvi(nbar, nodata, dtype, units):
    nbar_masked = mask_valid_data(nbar)
    ndvi_array = (nbar_masked.nir - nbar_masked.red) / (nbar_masked.nir + nbar_masked.red)
    ndvi_out = (ndvi_array * 10000).fillna(nodata).astype(dtype)
    ndvi_out.attrs = {
        'crs': nbar.attrs['crs'],
        'units': units,
        'nodata': nodata,
    }

    ndvi = xarray.Dataset({'ndvi': ndvi_out}, attrs=nbar.attrs)
    return ndvi


def do_ndvi_task(config, task):
    global_attributes = config['global_attributes']
    variable_params = config['variable_params']
    file_path = Path(task['filename'])
    output_type = config['ndvi_dataset_type']
    measurement = output_type.measurements['ndvi']
    output_dtype = np.dtype(measurement['dtype'])
    nodata_value = np.dtype(output_dtype).type(measurement['nodata'])

    if file_path.exists():
        raise OSError(errno.EEXIST, 'Output file already exists', str(file_path))

    measurements = ['red', 'nir']

    nbar_tile = task['nbar']
    nbar = GridWorkflow.load(nbar_tile, measurements)

    ndvi = calculate_ndvi(nbar, nodata=nodata_value, dtype=output_dtype, units=measurement['units'])

    def _make_dataset(labels, sources):
        assert len(sources)
        geobox = nbar.geobox
        source_data = union_points(*[dataset.extent.to_crs(geobox.crs).points for dataset in sources])
        valid_data = intersect_points(geobox.extent.points, source_data)
        dataset = make_dataset(product=output_type,
                               sources=sources,
                               extent=geobox.extent,
                               center_time=labels['time'],
                               uri=file_path.absolute().as_uri(),
                               app_info=get_app_metadata(config),
                               valid_data=GeoPolygon(valid_data, geobox.crs))
        return dataset

    datasets = xr_apply(nbar_tile.sources, _make_dataset, dtype='O')
    ndvi['dataset'] = datasets_to_doc(datasets)

    write_dataset_to_netcdf(
        dataset=ndvi,
        filename=Path(file_path),
        global_attributes=global_attributes,
        variable_params=variable_params,
    )
    return datasets


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


APP_NAME = 'ndvi'


@click.command(name=APP_NAME)
@ui.pass_index(app_name=APP_NAME)
@click.option('--dry-run', is_flag=True, default=False, help='Check if output files already exist')
@click.option('--year', callback=validate_year, help='Limit the process to a particular year')
@click.option('--backlog', type=click.IntRange(1, 100000), default=3200, help='Number of tasks to queue at the start')
@task_app_options
@task_app(make_config=make_ndvi_config, make_tasks=make_ndvi_tasks)
def ndvi_app(index, config, tasks, executor, dry_run, backlog, *args, **kwargs):
    click.echo('Starting NDVI processing...')

    if dry_run:
        check_existing_files((task['filename'] for task in tasks))
        return 0

    results = []
    tasks_backlog = itertools.islice(tasks, backlog)
    for task in tasks_backlog:
        _LOG.info('Queuing task: %s', task['tile_index'])
        results.append(executor.submit(do_ndvi_task, config=config, task=task))
    click.echo('Backlog queue filled, waiting for first result...')

    successful = failed = 0
    while results:
        result, results = executor.next_completed(results, None)

        # submit a new task to replace the one we just finished
        task = next(tasks, None)
        if task:
            _LOG.info('Queuing task: %s', task['tile_index'])
            results.append(executor.submit(do_ndvi_task, config=config, task=task))

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
    ndvi_app()
