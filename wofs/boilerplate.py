"""
This module encapsulates machinery to translate the WOFL algorithm into
an application for automating WOFL production (in an "operations" context).
"""


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

info = {'lineage': { 'algorithm': { 'name': "WOFS decision-tree water extents",
                                    'version': 'unknown',
                                    'repo_url': 'https://github.com/benjimin/wetness' }}}

bands = ['blue','green','red','nir','swir1','swir2'] # inputs needed from EO data

sensor = {'ls8':'LS8_OLI', 'ls7':'LS7_ETM', 'ls5':'LS5_TM'} # { nbar-prefix : filename-prefix } for platforms

destination = '/short/v10/datacube/wofs'
filename_template = '{sensor}_WATER/{tile_index[0]}_{tile_index[1]}/' + \
                    '{sensor}_WATER_3577_{tile_index[0]}_{tile_index[1]}_{time}.nc'
                    # note 3577 refers to the CRS (EPSG) of the definition below

def unpickle_stream(pickle_file):
    """Utility to stream unpickled objects from a file"""
    s = pickle.Unpickler(pickle_file)
    while True:
        try:
            yield s.load()
        except EOFError:
            raise StopIteration
            
def map_orderless(core,tasks,queue=50):
    """Utility to stream tasks through compute resources"""
    import distributed # slow import
    ex = distributed.Client() # executor  
    
    tasks = (i for i in tasks) # ensure input is a generator
      
    # pre-fill queue
    results = [ex.submit(core,*t) for t in itertools.islice(tasks, queue)]
           
    while results:
        result = next(distributed.as_completed(results)) # block
        results.remove(result)                  # pop completed

        task = next(tasks, None)
        if task is not None:
            results.append(ex.submit(core,*task)) # queue another
        
        yield result.result() # unwrap future

def get_product(index, definition):
    """Utility to get database-record corresponding to product-definition"""
    parsed = yaml.load(definition)
    metadata_type = index.metadata_types.get_by_name(parsed['metadata_type'])
    prototype = datacube.model.DatasetType(metadata_type, parsed)
    return index.products.add(prototype) # idempotent
    
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




class datacube_application:
    """Nonspecific application workflow."""
    info = NotImplemented
    def generate_tasks(self, index, time_range):
        """Prepare stream of tasks (i.e. of argument tuples)."""
        raise NotImplemented
    def perform_task(self, *args):
        """Execute computation without database interaction"""
        raise NotImplemented
    def __init__(self, time, **extent):
        """Collect keyword options"""
        self.default_time_range = time
        self.default_spatial_extent = extent
        with open('global_attributes.yaml') as f:
            self.global_attributes = yaml.load(f)
        self.product_definition = open('product_definition.yaml').read()
    def __call__(self, algorithm):
        """Annotator API for application
        
        >>> @datacube_application(**options)
        >>> def myfunction(input_chunk):
        >>>     return output_chunk
        """
        index = datacube.Datacube().index        
        self.core = algorithm
        self.product = get_product(index, self.product_definition)
        self.main(index)
        raise SystemExit
    def main(self, index):
        """Compatibility command-line-interface"""
        
        @click.group(name=self.core.__name__)
        def cli():
            pass
        
        @cli.command(help="Pre-query tiles for one calendar year.")
        @click.argument('year', type=click.INT)
        @click.argument('taskfile', type=click.File('w'))
        @click.option('--max', default=0, help="Limit number of tasks")
        def prepare(year, taskfile, max):
            t = str(year)+'-01-01', str(year+1)+'-01-01'
            print "Querying", t[0], "to", t[1]
            stream = pickle.Pickler(taskfile)
            i = 0
            for task in self.generate_tasks(index, time=t):
                stream.dump(task)
                i += 1
                if i==max:
                    break
            print i, "tasks prepared"         
           
        @cli.command(help="Read pre-queried tiles and distribute computation.")
        @click.option('--backlog', default=50, help="Maximum queue length")
        @click.argument('taskfile', type=click.File('r'))
        def orchestrate(backlog, taskfile):
            tasks = unpickle_stream(taskfile)
            done_tasks = map_orderless(self.perform_task, tasks, queue=backlog)
            for i,ds in enumerate(done_tasks):
                print i
                index.datasets.add(ds, skip_sources=True) # index completed work
            print "Done"
        
        @cli.command(help="Query and execute in single thread")
        @click.argument('year', type=click.INT)
        @click.option('--max', default=0, help="Limit number of tasks")
        def debug(year, max):
            t = str(year)+'-01-01', str(year+1)+'-01-01'
            print "Querying", t[0], "to", t[1]
            i = 0
            for task in self.generate_tasks(index, time=t):
                i += 1
                print i
                ds = self.perform_task(*task)
                index.datasets.add(ds, skip_sources=True) # index completed work
                if i==max:
                    break
            print "Done" 
               
        cli()




class wofloven(datacube_application):
    """Specialisations for Water Observation product"""
    info = info
    def generate_tasks(self, index, time, extent={}):
        """ Yield loadables (nbar,ps,dsm) and targets, for dispatch to workers.

        This function is the equivalent of an SQL join query,
        and is required as a workaround for datacube API abstraction layering.        
        """
        gw = datacube.api.GridWorkflow(index, product=self.product.name) # GridSpec from product definition

        wofls_loadables = gw.list_tiles(product=self.product.name, time=time, **extent)

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

    def perform_task(self, loadables, file_path):
        """ Load data, run WOFS algorithm, attach metadata, and write output.
        
        Input: 
            - three-tuple of Tile objects (NBAR, PQ, DSM)
            - path object (output file destination)
        Output:
            - indexable object (referencing output data location)
        """        
        if file_path.exists():
            raise OSError(errno.EEXIST, 'Output file already exists', str(file_path))
            
        # load data
        protosource, protopq, protodsm = loadables
        load = datacube.api.GridWorkflow.load
        source = load(protosource, measurements=bands)
        pq = load(protopq)
        dsm = load(protodsm, resampling='cubic')
        
        # Core computation
        result = self.core(*(x.isel(time=0) for x in [source, pq, dsm]))
        
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
                            product=self.product,
                            sources=allsources,
                            center_time=result.time.values[0],
                            uri=file_path.absolute().as_uri(),
                            extent=box,
                            valid_data=envelope,
                            app_info=self.info )   
                            
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

        return new_record
        

        