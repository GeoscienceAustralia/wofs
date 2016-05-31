#################################################
# Usage:
#   export PYTHONPATH=.:/g/data/u46/fxz547/Githubz/agdc-v2
#   python tests/testagdcv2_env.py
#################################################

import os, sys
from collections import defaultdict

import xarray as xr
import xarray.ufuncs

from datacube.api import API
from datacube.index import index_connect
from datacube.config import LocalConfig
from datacube.api._conversion import to_datetime
from datacube.api import make_mask

from wofs.waters.detree import WaterClassifier
import wofs.waters.detree.filters as filters
import numpy
from scipy import stats

import logging

logging.basicConfig(level=logging.INFO)


class AgdcDao():
    """
    Datacube Data Access Object. Default prod
    """

    def __init__(self, force_prod=True):

        if force_prod:
            prod_config = LocalConfig.find(['/g/data/v10/public/modules/agdc-py2-prod/1.0.2/datacube.conf'])
            prod_index = index_connect(prod_config, application_name='api-WOfS-dev')
            self.dao = API(prod_index)
        else:
            self.dao = API(application_name='api-WOfS-dev')

        return

    def get_cells_list(self, qdict):
        """
        Get a List of all relevant cell indexes by a dictionary qdict
        cells = dc.list_cells(product='nbar', longitude=lon_range, latitude=lat_range, time=time_interval, platform=satellites)

        all NBAR cells dc.list_cells(product='nbar')

        dc.list_cells(product='nbar', longitude=149, latitude=-35, time=('1990', '2015'))
        :return:
        """

        dc = self.dao  # API(app='WOfS-dev')

        # Query parameters:

        logging.debug(qdict)

        # {"product": "nbar", "longitude": (149, 150), "latitude": (-40, -41), "time": ('1900-01-01', '2016-03-20')}

        cells = dc.list_cells(
            **qdict)  # ** unpack a dict into a key-value pair argument matching the function's signature

        logging.info(str(cells))
        # return cells_filtered_list =filter_cell_list(cells)
        return cells

    def get_tiles_by_cell(self, cells, qdict):  # ,products=None, time_range=None, platforms=None):
        """
        query to discover all relevant tilesets (nbar + pqa) for a given cell_Id list
        :return: product_tiles
        """

        tile_store = self.get_nbarpqa_tiles(cells, qdict)

        for cell, stack in tile_store.items():
            product_tiles = []

            for time in sorted(stack):
                tileset = stack[time]  # This should be a pair of nbar-pqa
                if 'nbar' in tileset and 'pqa' in tileset:
                    logging.debug("This cell has both nbar and pq data at time=%s" % str(time))
                    logging.debug("%s, %s, %s", cell, to_datetime(time), len(tileset))  # , type(tiles)

                    nbar_pqa_pair = []
                    for product, tile in tileset.items():
                        nbar_pqa_pair.append((product, tile))

                    product_tiles.append(nbar_pqa_pair)
                else:
                    logging.warn("Skipping ....nbar-pqa mismatching tiles at time=%s", time)

        return product_tiles

    ##----------------------------------------------------------------
    def get_tiles_for_wofs_inputs(self, cells, qdict, inputs_dir):
        """
            query to discover all relevant tilesets (nbar + pqa) for a list of cells, write them onto inputs_dir/cell_id
            :return: product_tiles
            """

        tile_store = self.get_nbarpqa_tiles(cells, qdict)

        for cell, stack in tile_store.items():
            cell_tiles = []

            for time in sorted(stack):
                tileset = stack[time]  # This should be a pair of nbar-pqa
                if 'nbar' in tileset and 'pqa' in tileset:
                    logging.debug("This cell has both nbar and pq data at time=%s" % str(time))
                    logging.debug("%s, %s, %s", cell, to_datetime(time), len(tileset))  # , type(tiles)

                    nbar_pqa_pair = []
                    for product, tile in tileset.items():
                        nbar_pqa_pair.append((product, tile))

                    cell_tiles.append(nbar_pqa_pair)
                else:
                    logging.warn("Cell = %s : nbar-pqa mismatching tiles at time=%s .. Skipping", str(cell), time)

            cell_id = "abc%s_%s.txt" % (cell)  # a txt/csv filename based on albers cellindex
            fname = os.path.join(inputs_dir, cell_id)
            with  open(fname, 'w') as infile:
                for eachtile in cell_tiles:
                    infile.write(str(eachtile) + "\n")

        return

    ##----------------------------------------------------------------
    def get_nbarpqa_tiles(self, cells, qdict):
        """
        return a list of tiles
        :param cells: a list of cell index [(15, -40), ] with one or more element
        :return:
        """

        dc = self.dao

        nbar_tiles = dc.list_tiles(cells, product='nbar', **qdict)  # , platform='LANDSAT_8')  # ,time=('2000', '2007'))
        pq_tiles = dc.list_tiles(cells, product='pqa', **qdict)  # , platform='LANDSAT_8')  # , time=('2000', '2007'))

        if (len(pq_tiles) == len(nbar_tiles)):
            logging.debug("The cells have %s nbar and %s pq tiles", len(nbar_tiles), len(pq_tiles))
        else:
            logging.warn("NBAR-PQA tiles mismatch: The cells have %s nbar and %s pq tiles", len(nbar_tiles), len(pq_tiles))

        tile_store = defaultdict(lambda: defaultdict(dict))

        # for tile_query, tile_info in nbar_tiles:
        #     cell = tile_query['xy_index']
        #     time = tile_query['time']
        #     product = tile_info['metadata']['product_type']
        #     tile_store[cell][time][product] = (tile_query, tile_info)

        for tile_query, tile_info in nbar_tiles + pq_tiles:
            cells = tile_query['xy_index']
            time = tile_query['time']
            product = tile_info['metadata']['product_type']
            platform = tile_info['metadata']['platform']['code']
            path2file = tile_info.get('path')
            tile_store[cells][time][product] = (tile_query, platform, path2file)

        return tile_store

######################################################
    def get_nbarpq_data(self, cellindex, qdict):
        """
        for a given cell-index, and query qdict, return dataarrays of nbar and pqa tile-pair
        :param cellindex: = (15, -40)
        :return: list of tiles-pairs [(nbar,pq), ]
        """

        print cellindex

        # sys.exit(10)

        tiledatas = []

        tile_store = self.get_nbarpqa_tiles([cellindex], qdict)

        stack = tile_store[cellindex]

        for time in sorted(stack):
            # print ("time=", time)

            tileset = stack[time]

            # print "Cell {} at time [{:%Y-%m-%d}] has {} tiles: ".format(cellindex, to_datetime(time), len(tileset))
            # for product, tile in tileset.items():
            #     print product, tile

            if 'nbar' in tileset and 'pqa' in tileset:

                logging.info("This cell has both nbar and pq data at time=%s" % str(time))

                nbar_tile_query, platform, path2file = tileset['nbar']
                # This will get replaced by the semantic layer
                if platform in ('LANDSAT_5', 'LANDSAT_7'):
                    variables = ['band_1', 'band_2', 'band_3', 'band_4', 'band_5', 'band_7']
                elif platform in ('LANDSAT_8'):
                    variables = ['band_2', 'band_3', 'band_4', 'band_5', 'band_6', 'band_7']

                # nbar_tile = self.dao.get_data_array_by_cell(variables=variables, set_nan=True, **nbar_tile_query)
                nbar_tile = self.dao.get_data_array_by_cell(variables=variables, set_nan=False, **nbar_tile_query)

                pq_tile_query,  platform, path2file = tileset['pqa']
                pq_tile_ds = self.dao.get_dataset_by_cell(**pq_tile_query)
                pq_tile = pq_tile_ds['pixelquality']

                # print "{:%c}\tnbar shape: {}\tpq shape: {}".format(to_datetime(time), nbar_tile.shape, pq_tile.shape)
                # Wed Dec 27 23:45:28 2006	nbar shape: (6, 4000, 4000)	pq shape: (4000, 4000)

                tiledatas.append((time, platform, nbar_tile, pq_tile))

                # break  # use break if just do the first one as a test...
            else:
                logging.warn("WARN missing data at time=%s", time)

        return tiledatas


###############################################################################################
def comput_img_stats(waterimg):
    """
    compute and show the pixel values, etc of a 1-band image, typically classfied image with water extent
    :param waterimg:
    :return:
    """

    print waterimg.shape

    nowater_pix = numpy.sum(waterimg == 0)  # not water
    water_pix = numpy.sum(waterimg == 128)  # water
    nodata_pix = numpy.sum(waterimg == 1)  # water_extent nodata==1

    totatl_pix = nowater_pix + water_pix + nodata_pix

    print nowater_pix, water_pix, nodata_pix, totatl_pix

    wimg1d = waterimg.flat

    # for i in range(0, len(wimg1d)):
    #     if (wimg1d[i] != 0) and (wimg1d[i] != 128):
    #         print i, wimg1d[i]

    print stats.describe(wimg1d)


def write_img(waterimg, geometa, path2file):
    """
     write output the numpy array waterimg into a netcdf file?
    :param waterimg:
    :param geometa:

    :param path2file:
    :return:
    """
    xrarr = xr.DataArray(waterimg, name=geometa["name"])

    xrds = xrarr.to_dataset()
    xrds.to_netcdf(path2file)

    return path2file


################################################################################################
# ----------------------------------------------------------
if __name__ == "__main__":

    qdict={'latitude': (-36.0, -35.0), 'platform': ['LANDSAT_5', 'LANDSAT_7', 'LANDSAT_8'], 'longitude': (149.01, 150.1), 'time': ('2000-01-01', '2016-03-31')}
    qdict = {'latitude': (-36.0, -35.0), 'platform': ['LANDSAT_8'], 'longitude': (149.01, 150.1), 'time': ('2000-01-01', '2016-03-31')}

    dcdao = AgdcDao()

    cellindex = (15, -40)
    cellindex = (15, -41)
    tile_data = dcdao.get_nbarpq_data(cellindex, qdict)

    icounter = 0
    # for (t, nbar, pq) in tile_dat:
    for (t, platform, nbar, pq) in tile_data[:3]:  # do the first few tiles of the list

        print (t, platform, nbar, pq)
        print (type(nbar), type(pq))
        print (nbar.shape, pq.shape)


# Output will look like:
#
# (1365723995.385577, u'LANDSAT_8', <xarray.DataArray u'ls8_nbar_albers' (variable: 6, y: 4000, x: 4000)>
# dask.array<concate..., shape=(6, 4000, 4000), dtype=int16, chunksize=(1, 4000, 4000)>
# Coordinates:
#     time      datetime64[ns] 2013-04-11T23:46:35.385577
#   * y         (y) float64 -4e+06 -4e+06 -4e+06 -4e+06 -4e+06 -4e+06 -4e+06 ...
#   * x         (x) float64 1.5e+06 1.5e+06 1.5e+06 1.5e+06 1.5e+06 1.5e+06 ...
#   * variable  (variable) <U6 u'band_2' u'band_3' u'band_4' u'band_5' ...
# Attributes:
#     _FillValue: -999, <xarray.DataArray 'pixelquality' (y: 4000, x: 4000)>
# dask.array<getitem..., shape=(4000, 4000), dtype=int16, chunksize=(4000, 4000)>
# Coordinates:
#     time     datetime64[ns] 2013-04-11T23:46:35.385577
#   * y        (y) float64 -4e+06 -4e+06 -4e+06 -4e+06 -4e+06 -4e+06 -4e+06 ...
#   * x        (x) float64 1.5e+06 1.5e+06 1.5e+06 1.5e+06 1.5e+06 1.5e+06 ...
# Attributes:
#     units: 1
#     long_name: Quality Control
#     flags_definition: {u'cloud_shadow_acca': {u'bit_index': 12, u'description': u'Cloud Shadow (ACCA)', u'value': 0}, u'cloud_acca': {u'bit_index': 10, u'description': u'Cloud (ACCA)', u'value': 0}, u'land_obs': {u'bit_index': 9, u'description': u'Land observation', u'value': 1}, u'band_1_saturated': {u'bit_index': 0, u'description': u'Band 1 is saturated', u'value': 0}, u'contiguity': {u'bit_index': 8, u'description': u'All bands for this pixel contain non-null values', u'value': 1}, u'band_2_saturated': {u'bit_i...)
# (<class 'xarray.core.dataarray.DataArray'>, <class 'xarray.core.dataarray.DataArray'>)
# ((6, 4000, 4000), (4000, 4000))


# (1365724019.356951, u'LANDSAT_8', <xarray.DataArray u'ls8_nbar_albers' (variable: 6, y: 4000, x: 4000)>
# dask.array<concate..., shape=(6, 4000, 4000), dtype=int16, chunksize=(1, 4000, 4000)>
# Coordinates:
#     time      datetime64[ns] 2013-04-11T23:46:59.356951
#   * y         (y) float64 -4e+06 -4e+06 -4e+06 -4e+06 -4e+06 -4e+06 -4e+06 ...
#   * x         (x) float64 1.5e+06 1.5e+06 1.5e+06 1.5e+06 1.5e+06 1.5e+06 ...
#   * variable  (variable) <U6 u'band_2' u'band_3' u'band_4' u'band_5' ...
# Attributes:
#     _FillValue: -999, <xarray.DataArray 'pixelquality' (y: 4000, x: 4000)>
# dask.array<getitem..., shape=(4000, 4000), dtype=int16, chunksize=(4000, 4000)>
# Coordinates:
#     time     datetime64[ns] 2013-04-11T23:46:59.356951
#   * y        (y) float64 -4e+06 -4e+06 -4e+06 -4e+06 -4e+06 -4e+06 -4e+06 ...
#   * x        (x) float64 1.5e+06 1.5e+06 1.5e+06 1.5e+06 1.5e+06 1.5e+06 ...
# Attributes:
#     units: 1
#     long_name: Quality Control
#     flags_definition: {u'cloud_shadow_acca': {u'bit_index': 12, u'description': u'Cloud Shadow (ACCA)', u'value': 0}, u'cloud_acca': {u'bit_index': 10, u'description': u'Cloud (ACCA)', u'value': 0}, u'land_obs': {u'bit_index': 9, u'description': u'Land observation', u'value': 1}, u'band_1_saturated': {u'bit_index': 0, u'description': u'Band 1 is saturated', u'value': 0}, u'contiguity': {u'bit_index': 8, u'description': u'All bands for this pixel contain non-null values', u'value': 1}, u'band_2_saturated': {u'bit_i...)
# (<class 'xarray.core.dataarray.DataArray'>, <class 'xarray.core.dataarray.DataArray'>)
# ((6, 4000, 4000), (4000, 4000))
