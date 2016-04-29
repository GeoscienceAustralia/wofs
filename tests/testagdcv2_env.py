#################################################
# Usage:
#   export PYTHONPATH=/g/data/u46/fxz547/Githubz/agdc-v2
#   python tests/testagdcv2_env.py
#################################################
from IPython.display import display
from collections import defaultdict

import xarray as xr
import xarray.ufuncs

from datacube.api import API
from datacube.index import index_connect
from datacube.config import LocalConfig
from datacube.api._conversion import to_datetime
from datacube.api import make_mask, describe_flags


force_prod = True
if force_prod:
    prod_config = LocalConfig.find(['/g/data/v10/public/modules/agdc-py2-prod/1.0.2/datacube.conf'])
    prod_index = index_connect(prod_config, application_name='api-WOfS-dev')
    dc = API(prod_index)
else:
    dc = API(application_name='api-WOfS-dev')


print dc.list_cells(product='nbar', longitude=149, latitude=-35, time=('1990', '2015'))


cells = [(-15, -40)]


nbar_tiles = dc.list_tiles(cells, product='nbar', platform='LANDSAT_5')

print len(nbar_tiles)

pq_tiles = dc.list_tiles(cells, product='pqa', platform='LANDSAT_5', time=('2000', '2010'))

print len(pq_tiles)

tile_store = defaultdict(lambda: defaultdict(dict))

for tile_query, tile_info in nbar_tiles:
    cell = tile_query['xy_index']
    time = tile_query['time']
    product = tile_info['metadata']['product_type']
    tile_store[cell][time][product] = (tile_query, tile_info)

for tile_query, tile_info in pq_tiles:
    cell = tile_query['xy_index']
    time = tile_query['time']
    product = tile_info['metadata']['product_type']
    tile_store[cell][time][product] = (tile_query, tile_info)

for cell, stack in tile_store.items():
    for time in sorted(stack):
        tiles = stack[time]
        print "Cell {} at time [{:%Y-%m-%d}] has {} tiles".format(cell, to_datetime(time), len(tiles))
        for product, tile in tiles.items():
            print '\t', product


stack = tile_store[cell]

for time in sorted(stack):
    tileset = stack[time]

    
    if 'nbar' in tileset and 'pqa' in tileset:
        print ("This cell has both nbar and pq data at time %s"% str(time))
    else:
        print "not a good time-sliced tile - we have missing data!"

    nbar_tile_query, nbar_tile_info = tileset['nbar']
    # This will get replaced by the semantic layer
    platform = nbar_tile_info['metadata']['platform']['code']
    if platform in ('LANDSAT_5', 'LANDSAT_7'):
        variables = ['band_1', 'band_2', 'band_3', 'band_4', 'band_5', 'band_7']
    elif platform in ('LANDSAT_8'):
        variables = ['band_2', 'band_3', 'band_4', 'band_5', 'band_6', 'band_8']


    nbar_tile = dc.get_data_array_by_cell(variables=variables, set_nan=True, **nbar_tile_query)

    no_data_tile = (~xr.ufuncs.isfinite(nbar_tile)).any(dim='variable')

    pq_tile_query, pq_tile_info = tileset['pqa']
    pq_tile =  dc.get_dataset_by_cell(**pq_tile_query)

    print "{:%c}\tnbar shape: {}\tpq shape: {}".format(to_datetime(time), nbar_tile.shape, pq_tile['pixelquality'].shape)

    break # Just do the first one as a test...


pic=nbar_tile[0].plot()  #0= blue band
