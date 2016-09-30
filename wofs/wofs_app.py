"""
Copied from the NDVI/FC template, this is a quick and dirty approach to 
producing a datacube application with full compatibility.
"""


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

import datacube
import pathlib
import errno 
import xarray
import pandas
import datacube.model.utils
import yaml
import click
import pickle
import itertools
from wofs import wofls

_LOG = logging.getLogger('agdc-wofs')

bands = ['blue','green','red','nir','swir1','swir2'] # inputs needed from EO data
sensor = {'ls8':'LS8_OLI', 'ls7':'LS7_ETM', 'ls5':'LS5_TM'} # { nbar-prefix : filename-prefix } for platforms
filename_template = '{sensor}_WATER/{tile_index[0]}_{tile_index[1]}/' + \
                    '{sensor}_WATER_3577_{tile_index[0]}_{tile_index[1]}_{time}.nc'
                    # note 3577 refers to the CRS (EPSG) of the definition below

def get_product(index, definition, dry_run=False):
    """Utility to get database-record corresponding to product-definition"""
    parsed = definition
    metadata_type = index.metadata_types.get_by_name(parsed['metadata_type'])
    prototype = datacube.model.DatasetType(metadata_type, parsed)
    return prototype if dry_run else index.products.add(prototype) # add is idempotent

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
        _LOG.info('Created DatasetType %s', 
                  config['product_definition']['name']) # true? nyet.
        
    config['wofs_dataset_type'] = \
        get_product(index, config['product_definition'], dry_run)

    if not os.access(config['location'], os.W_OK):
        _LOG.warn('Current user appears not have write access output location: %s', config['location'])

    return config


def get_filename(config, tile_index, sources):
    file_path_template = str(Path(config['location'], config['file_path_template']))
    return file_path_template.format(tile_index=tile_index,
                                     start_time=to_datetime(sources.time.values[0]).strftime('%Y%m%d%H%M%S%f'),
                                     end_time=to_datetime(sources.time.values[-1]).strftime('%Y%m%d%H%M%S%f'))

def generate_tasks(index, config, time):
    """ Yield loadables (nbar,ps,dsm) and targets, for dispatch to workers.

    This function is the equivalent of an SQL join query,
    and is required as a workaround for datacube API abstraction layering.        
    """
    extent={} # not configurable
    product = config['wofs_dataset_type']
    destination = config['location']
    
    gw = datacube.api.GridWorkflow(index, product=product.name) # GridSpec from product definition

    wofls_loadables = gw.list_tiles(product=product.name, time=time, **extent)

    for platform in sensor.keys():
        source_loadables = gw.list_tiles(product=platform+'_nbar_albers', time=time, **extent)
        pq_loadables = gw.list_tiles(product=platform+'_pq_albers', time=time, **extent)
        dsm_loadables = gw.list_tiles(product='dsm1sv10', **extent)                

        assert len(set(t for (x,y,t) in dsm_loadables)) == 1 # assume mosaic won't require extra fusing
        dsm_loadables = {(x,y):val for (x,y,t),val in dsm_loadables.items()} # make mosaic atemporal

        # only valid where EO, PQ and DSM are *all* available (and WOFL isn't yet)
        keys = set(source_loadables) & set(pq_loadables) .difference(set(wofls_loadables))
        # should sort spatially, consider repartitioning workload to minimise DSM reads.
        for x,y,t in keys:
            if (x,y) in dsm_loadables: # filter complete
                fn = filename_template.format(sensor=sensor[platform],
                                              tile_index=(x,y),
                                              time=pandas.to_datetime(t).strftime('%Y%m%d%H%M%S%f'))
                s,p,d = map(gw.update_tile_lineage, # fully flesh-out the metadata
                            [ source_loadables[(x,y,t)], pq_loadables[(x,y,t)], dsm_loadables[(x,y)] ])
                yield ((s,p,d), pathlib.Path(destination,fn))


def make_wofs_tasks(index, config, year=None, **kwargs):
    # TODO: Filter query to valid options
    query = {}
    if year is not None:
        if isinstance(year, integer_types):
            query['time'] = Range(datetime(year=year, month=1, day=1), datetime(year=year+1, month=1, day=1))
        elif isinstance(year, tuple):
            query['time'] = Range(datetime(year=year[0], month=1, day=1), datetime(year=year[1]+1, month=1, day=1))

    tasks = list(generate_tasks(index, config, time=query['time']))

    _LOG.info('%s tasks discovered', len(tasks))
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

def box_and_envelope(loadables):
    """Utility to prepare spatial metadata"""
    # Tile loadables contain a "sources" DataArray, that is, a time series 
    # (in this case with unit length) of tuples (lest fusing may be necessary)
    # of datacube Datasets, which should each have memoised a file path
    # (extracted from the database) as well as an array extent and a valid 
    # data extent. (Note both are just named "extent" inconsistently.)
    # The latter exists as an optimisation to sometimes avoid loading large 
    # volumes of (exclusively) nodata values. 
    #assert len(set(x.geobox.extent for x in loadables)) == 1 # identical geoboxes are unequal?
    bounding_box = loadables[0].geobox.extent # inherit array-boundary from post-load data
    def valid_data_envelope(loadables=list(loadables), crs=bounding_box.crs):
        def data_outline(tile):
            parts = (ds.extent.to_crs(crs).points for ds in tile.sources.values[0])
            return datacube.utils.union_points(*parts)
        footprints = [bounding_box.points] + map(data_outline, loadables)
        overlap = reduce(datacube.utils.intersect_points, footprints)
        return datacube.model.GeoPolygon(overlap, crs)    
    return bounding_box, valid_data_envelope()

def docvariable(agdc_dataset, time):
    """Utility to convert datacube dataset to xarray/NetCDF variable"""
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
    global_attributes = config['global_attributes']
    app_info = get_app_metadata(config)
    core = wofls.woffles 
       
    
    if file_path.exists():
        raise OSError(errno.EEXIST, 'Output file already exists', str(file_path))
        
    # load data
    protosource, protopq, protodsm = loadables
    load = datacube.api.GridWorkflow.load
    source = load(protosource, measurements=bands)
    pq = load(protopq)
    dsm = load(protodsm, resampling='cubic')
    
    # Core computation
    result = core(*(x.isel(time=0) for x in [source, pq, dsm]))
    
    # Convert 2D DataArray to 3D DataSet
    result = xarray.concat([result], source.time).to_dataset(name='water')
    
    # add metadata
    result.water.attrs['nodata'] = 1 # lest it default to zero (i.e. clear dry)
    result.water.attrs['units'] = '1' # unitless (convention)

    # Attach CRS. Note this is poorly represented in NetCDF-CF
    # (and unrecognised in xarray), likely improved by datacube-API model.
    result.attrs['crs'] = source.crs
    
    # inherit spatial metadata
    box, envelope = box_and_envelope(loadables)

    # Provenance tracking
    allsources = [ds for tile in loadables for ds in tile.sources.values[0]]

    # Create indexable record
    new_record = datacube.model.utils.make_dataset(
                        product=product,
                        sources=allsources,
                        center_time=result.time.values[0],
                        uri=file_path.absolute().as_uri(),
                        extent=box,
                        valid_data=envelope,
                        app_info=app_info )   
                        
    # inherit optional metadata from EO, for future convenience only
    def harvest(what, datasets=[ds for time in protosource.sources.values for ds in time]):
        values = [ds.metadata_doc[what] for ds in datasets]
        assert all(value==values[0] for value in values)
        return values[0]
    new_record.metadata_doc['platform'] = harvest('platform') 
    new_record.metadata_doc['instrument'] = harvest('instrument') 
    
    # copy metadata record into xarray 
    result['dataset'] = docvariable(new_record, result.time)

    # write output
    datacube.storage.storage.write_dataset_to_netcdf(
        result, file_path, global_attributes=self.global_attributes)

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
@click.option('--backlog', type=click.IntRange(1, 100000), default=3200, help='Number of tasks to queue at the start')
@task_app_options
@task_app(make_config=make_wofs_config, make_tasks=make_wofs_tasks)
def wofs_app(index, config, tasks, executor, dry_run, backlog, *args, **kwargs):
    click.echo('Starting processing...')

    if dry_run:
        check_existing_files((task['filename'] for task in tasks))
        return 0

    results = []
    tasks_backlog = itertools.islice(tasks, backlog)
    for task in tasks_backlog:
        _LOG.info('Queuing task: %s', task['tile_index'])
        results.append(executor.submit(do_wofs_task, config=config, task=task))
    click.echo('Backlog queue filled, waiting for first result...')

    successful = failed = 0
    while results:
        result, results = executor.next_completed(results, None)

        # submit a new task to replace the one we just finished
        task = next(tasks, None)
        if task:
            _LOG.info('Queuing task: %s', task['tile_index'])
            results.append(executor.submit(do_wofs_task, config=config, task=task))

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
