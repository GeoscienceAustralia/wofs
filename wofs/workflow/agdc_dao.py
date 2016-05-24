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
    def get_nbarpq_data_by_cell_index(self, cellindex):
        """
        :param cellindex: = (15, -40)
        :return: list of tiles-pairs [(nbar,pq), ]
        """

        print cellindex

        # sys.exit(10)

        tiledatas = []

        tile_store = self.get_nbarpqa_tiles([cellindex])  # [].append(cellindex))

        stack = tile_store[cellindex]

        for time in sorted(stack):
            # print ("time=", time)

            tileset = stack[time]

            # print "Cell {} at time [{:%Y-%m-%d}] has {} tiles: ".format(cellindex, to_datetime(time), len(tileset))
            # for product, tile in tileset.items():
            #     print product, tile

            if 'nbar' in tileset and 'pqa' in tileset:

                logging.info("This cell has both nbar and pq data at time=%s" % str(time))

                nbar_tile_query, nbar_tile_info = tileset['nbar']
                # This will get replaced by the semantic layer
                platform = nbar_tile_info['metadata']['platform']['code']
                if platform in ('LANDSAT_5', 'LANDSAT_7'):
                    variables = ['band_1', 'band_2', 'band_3', 'band_4', 'band_5', 'band_7']
                elif platform in ('LANDSAT_8'):
                    variables = ['band_2', 'band_3', 'band_4', 'band_5', 'band_6', 'band_7']

                # nbar_tile = self.dao.get_data_array_by_cell(variables=variables, set_nan=True, **nbar_tile_query)
                nbar_tile = self.dao.get_data_array_by_cell(variables=variables, set_nan=False, **nbar_tile_query)

                pq_tile_query, pq_tile_info = tileset['pqa']
                pq_tile_ds = self.dao.get_dataset_by_cell(**pq_tile_query)
                pq_tile = pq_tile_ds['pixelquality']

                # print "{:%c}\tnbar shape: {}\tpq shape: {}".format(to_datetime(time), nbar_tile.shape, pq_tile.shape)
                # Wed Dec 27 23:45:28 2006	nbar shape: (6, 4000, 4000)	pq shape: (4000, 4000)

                tiledatas.append((time, nbar_tile, pq_tile))

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

    wimg1d = water_classified_img.flat

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
    xrarr = xr.DataArray(water_classified_img, name=geometa["name"])

    xrds = xrarr.to_dataset()
    xrds.to_netcdf(path2file)

    return path2file


##############################################################################

if __name__ == "__main__B":

    dcdao = AgdcDao()

    cellindex = (15, -40)

    tile_dat = dcdao.get_nbarpq_data_by_cell_index(cellindex)

    classifier = WaterClassifier()

    icounter = 0
    for (t, nbar,
         pq) in tile_dat:  # acquisition_id,satellite,start_datetime,end_datetime,end_datetime_year,end_datetime_month,x_index,y_index,xy,datasets
        # 309601,LS7,2014-07-04 00:30:53,2014-07-04 00:31:17,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_NBAR_138_-035_2014-07-04T00-30-53.tif},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_PQA_138_-035_2014-07-04T00-30-53.tif}}"
        # 309653,LS7,2014-07-20 00:30:55,2014-07-20 00:31:19,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_NBAR_138_-035_2014-07-20T00-30-55.tif},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_PQA_138_-035_2014-07-20T00-30-55.tif}}"
        # 309661,LS7,2014-07-27 00:36:45,2014-07-27 00:37:09,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/mosaic_cache/LS7_ETM_NBAR_138_-035_2014-07-27T00-36-45.vrt},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/mosaic_cache/LS7_ETM_PQA_138_-035_2014-07-27T00-36-45.tif}}"
        # 309705,LS7,2014-07-11 00:37:04,2014-07-11 00:37:28,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_NBAR_138_-035_2014-07-11T00-37-04.tif},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_PQA_138_-035_2014-07-11T00-37-04.tif}}"

        # for (t, nbar, pq) in tile_dat[:3]: #do the first few tiles of the list
        print (t, nbar.shape, pq.shape)
        # print type(nbar), type(pq)# acquisition_id,satellite,start_datetime,end_datetime,end_datetime_year,end_datetime_month,x_index,y_index,xy,datasets
        # 309601,LS7,2014-07-04 00:30:53,2014-07-04 00:31:17,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_NBAR_138_-035_2014-07-04T00-30-53.tif},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_PQA_138_-035_2014-07-04T00-30-53.tif}}"
        # 309653,LS7,2014-07-20 00:30:55,2014-07-20 00:31:19,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_NBAR_138_-035_2014-07-20T00-30-55.tif},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_PQA_138_-035_2014-07-20T00-30-55.tif}}"
        # 309661,LS7,2014-07-27 00:36:45,2014-07-27 00:37:09,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/mosaic_cache/LS7_ETM_NBAR_138_-035_2014-07-27T00-36-45.vrt},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/mosaic_cache/LS7_ETM_PQA_138_-035_2014-07-27T00-36-45.tif}}"
        # 309705,LS7,2014-07-11 00:37:04,2014-07-11 00:37:28,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_NBAR_138_-035_2014-07-11T00-37-04.tif},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_PQA_138_-035_2014-07-11T00-37-04.tif}}"


        # (1138232422.2250061, (6, 4000, 4000), (4000, 4000))
        # class 'xarray.core.dataarray.DataArray'># acquisition_id,satellite,start_datetime,end_datetime,end_datetime_year,end_datetime_month,x_index,y_index,xy,datasets
        # 309601,LS7,2014-07-04 00:30:53,2014-07-04 00:31:17,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_NBAR_138_-035_2014-07-04T00-30-53.tif},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_PQA_138_-035_2014-07-04T00-30-53.tif}}"
        # 309653,LS7,2014-07-20 00:30:55,2014-07-20 00:31:19,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_NBAR_138_-035_2014-07-20T00-30-55.tif},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_PQA_138_-035_2014-07-20T00-30-55.tif}}"
        # 309661,LS7,2014-07-27 00:36:45,2014-07-27 00:37:09,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/mosaic_cache/LS7_ETM_NBAR_138_-035_2014-07-27T00-36-45.vrt},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/mosaic_cache/LS7_ETM_PQA_138_-035_2014-07-27T00-36-45.tif}}"
        # 309705,LS7,2014-07-11 00:37:04,2014-07-11 00:37:28,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_NBAR_138_-035_2014-07-11T00-37-04.tif},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_PQA_138_-035_2014-07-11T00-37-04.tif}}"


        # TODO: water classification using these xarray datas ++

        water_classified_img = classifier.classify(nbar.values)

        # 2 Nodata filter
        nodata_val = nbar.attrs["_FillValue"]

        print nodata_val

        water_classified_img = filters.NoDataFilter().apply(water_classified_img, nbar.values, nodata_val)

        print ("Verify the water classified image ")

        # verify that classified image is a 2D (4000X4000) 1 band image with values in defined domain {0,1,..., 128, }

        comput_img_stats(water_classified_img)

        #  save the image to a file: numpy data
        # https://www.google.com.au/webhp?sourceid=chrome-instant&ion=1&espv=2&ie=UTF-8#q=write%20numpy%20ndarray%20to%20file

        outfilename = "waterextent%s.nc" % (icounter)
        path2outf = os.path.join("/g/data1/u46/fxz547/wofs2/extents", outfilename)
        # water_classified_img.tofile(path2outf) #raw data numpy file

        geometadat = {"name": "waterextent", "ablersgrid_cellindex": cellindex}
        write_img(water_classified_img, geometadat, path2outf)
        icounter += 1


        #     no_data_img = (~xr.ufuncs.isfinite(nbar)).any(dim='variable')
        #
        # print "no_data_img type: " + str(type(no_data_img))
        #
        # import matplotlib.pyplot as pyplt
        # no_data_img.plot.imshow()
        # pyplt.show()
        #
        # print "end of program main"

####################################################
# Discovery cell.cdv
# acquisition_id,satellite,start_datetime,end_datetime,end_datetime_year,end_datetime_month,x_index,y_index,xy,datasets
# 309601,LS7,2014-07-04 00:30:53,2014-07-04 00:31:17,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_NBAR_138_-035_2014-07-04T00-30-53.tif},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_PQA_138_-035_2014-07-04T00-30-53.tif}}"
# 309653,LS7,2014-07-20 00:30:55,2014-07-20 00:31:19,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_NBAR_138_-035_2014-07-20T00-30-55.tif},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_PQA_138_-035_2014-07-20T00-30-55.tif}}"
# 309661,LS7,2014-07-27 00:36:45,2014-07-27 00:37:09,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/mosaic_cache/LS7_ETM_NBAR_138_-035_2014-07-27T00-36-45.vrt},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/mosaic_cache/LS7_ETM_PQA_138_-035_2014-07-27T00-36-45.tif}}"
# 309705,LS7,2014-07-11 00:37:04,2014-07-11 00:37:28,2014,7,138,-35,"(138,-35)","{{ARG25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_NBAR_138_-035_2014-07-11T00-37-04.tif},{PQ25,/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS7_ETM/138_-035/2014/LS7_ETM_PQA_138_-035_2014-07-11T00-37-04.tif}}"

# Water Mapping
# water_extent filename: Platform_Sensor_WATER_CELLID_DATETIMESTAMP
# LS5_TM_WATER_136_-032_1987-12-07T00-12-56.014088.tif
# LS7_ETM_WATER_136_-032_2000-03-05T00-37-07.703569.tif
# LS8_OLI_TIRS_WATER_136_-032_2013-04-11T00-42-50.tif
# ----------------------------------------------------------
if __name__ == "__main__":

    dcdao = AgdcDao()

    cellindex = (15, -40)
    cellindex = (15, -41)
    tile_dat = dcdao.get_nbarpq_data_by_cell_index(cellindex)

    icounter = 0
    # for (t, nbar, pq) in tile_dat:
    for (t, nbar, pq) in tile_dat[:3]:  # do the first few tiles of the list

        print (t, nbar, pq)
        print (type(nbar), type(pq))
        print (nbar.shape, pq.shape)


# Output will look like
#
# (1366329150.151186,
# NBAR:
# <xarray.DataArray u'ls8_nbar_albers' (variable: 6, y: 4000, x: 4000)>
# dask.array<concate..., shape=(6, 4000, 4000), dtype=int16, chunksize=(1, 4000, 4000)>
# Coordinates:
#     time      datetime64[ns] 2013-04-18T23:52:30.151186
#   * y         (y) float64 -3.9e+06 -3.9e+06 -3.9e+06 -3.9e+06 -3.9e+06 ...
#   * x         (x) float64 1.5e+06 1.5e+06 1.5e+06 1.5e+06 1.5e+06 1.5e+06 ...
#   * variable  (variable) <U6 u'band_2' u'band_3' u'band_4' u'band_5' ...
# Attributes:
#     _FillValue: -999,

# PQ:
# <xarray.DataArray 'pixelquality' (y: 4000, x: 4000)>
# dask.array<getitem..., shape=(4000, 4000), dtype=int16, chunksize=(4000, 4000)>
# Coordinates:
#     time     datetime64[ns] 2013-04-18T23:52:30.151186
#   * y        (y) float64 -3.9e+06 -3.9e+06 -3.9e+06 -3.9e+06 -3.9e+06 ...
#   * x        (x) float64 1.5e+06 1.5e+06 1.5e+06 1.5e+06 1.5e+06 1.5e+06 ...
# Attributes:
#     units: 1
#     long_name: Quality Control
#     flags_definition: {u'cloud_shadow_acca': {u'bit_index': 12, u'description': u'Cloud Shadow (ACCA)', u'value': 0}, u'cloud_acca': {u'bit_index': 10, u'description': u'Cloud (ACCA)', u'value': 0}, u'land_obs': {u'bit_index': 9, u'description': u'Land observation', u'value': 1}, u'band_1_saturated': {u'bit_index': 0, u'description': u'Band 1 is saturated', u'value': 0}, u'contiguity': {u'bit_index': 8, u'description': u'All bands for this pixel contain non-null values', u'value': 1}, u'band_2_saturated': {u'bit_i...)
#
# (<class 'xarray.core.dataarray.DataArray'>, <class 'xarray.core.dataarray.DataArray'>)
#
# ((6, 4000, 4000), (4000, 4000))
