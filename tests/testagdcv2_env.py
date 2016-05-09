#################################################
# Usage:
#   export PYTHONPATH=.:/g/data/u46/fxz547/Githubz/agdc-v2
#   python tests/testagdcv2_env.py
#################################################

from collections import defaultdict

import xarray as xr
import xarray.ufuncs

from datacube.api import API
from datacube.index import index_connect
from datacube.config import LocalConfig
from datacube.api._conversion import to_datetime
from datacube.api import make_mask, describe_flags

from wofs.waters.detree import WaterClassifier
import wofs.waters.detree.filters as filters
import numpy
from scipy import stats
import os


class DatcubeDao():
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

    def get_tile_store(self, cells):
        """
        return a dictionary of tiles
        :param cells:
        :return:
        """

        # eg, cells = [(15, -40)]  #cover Canberra

        dc = self.dao

        nbar_tiles = dc.list_tiles(cells, product='nbar', platform='LANDSAT_8')  # ,time=('2000', '2007'))
        pq_tiles = dc.list_tiles(cells, product='pqa', platform='LANDSAT_8')  # , time=('2000', '2007'))

        if (len(pq_tiles) == len(nbar_tiles)):
            print ("The cell %s has %s nbar and %s pq tiles" % (str(cells), len(nbar_tiles), len(pq_tiles)))
        else:
            print "WARNING: unequal number of nbar and pq tiles: "
            print ("The cell %s has %s nbar and %s pq tiles" % (str(cells), len(nbar_tiles), len(pq_tiles)))

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

        return tile_store

    ######################################################
    def get_nbarpq_data_by_cell_index(self, cellindex):
        """
        :param cellindex: = (15, -40)
        :return: list of tiles-pairs [(nbar,pq), ]
        """

        tiledatas = []

        tile_store = self.get_tile_store([].append(cellindex))

        stack = tile_store[cellindex]

        for time in sorted(stack):
            print ("time=", time)

            tileset = stack[time]

            print "Cell {} at time [{:%Y-%m-%d}] has {} tiles: ".format(cellindex, to_datetime(time), len(tileset))
            for product, tile in tileset.items():
                print product, tile

            if 'nbar' in tileset and 'pqa' in tileset:
                print ("This cell has both nbar and pq data at time %s" % str(time))
            else:
                print "not a good tile -  missing data!"

            nbar_tile_query, nbar_tile_info = tileset['nbar']
            # This will get replaced by the semantic layer
            platform = nbar_tile_info['metadata']['platform']['code']
            if platform in ('LANDSAT_5', 'LANDSAT_7'):
                variables = ['band_1', 'band_2', 'band_3', 'band_4', 'band_5', 'band_7']
            elif platform in ('LANDSAT_8'):
                variables = ['band_2', 'band_3', 'band_4', 'band_5', 'band_6', 'band_7']

            #nbar_tile = self.dao.get_data_array_by_cell(variables=variables, set_nan=True, **nbar_tile_query)
            nbar_tile = self.dao.get_data_array_by_cell(variables=variables, set_nan=False, **nbar_tile_query)

            pq_tile_query, pq_tile_info = tileset['pqa']
            pq_tile_ds = self.dao.get_dataset_by_cell(**pq_tile_query)
            pq_tile = pq_tile_ds['pixelquality']

            # print "{:%c}\tnbar shape: {}\tpq shape: {}".format(to_datetime(time), nbar_tile.shape, pq_tile.shape)
            # Wed Dec 27 23:45:28 2006	nbar shape: (6, 4000, 4000)	pq shape: (4000, 4000)

            tiledatas.append((time, nbar_tile, pq_tile))

            # break  # use break if just do the first one as a test...

        return tiledatas

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
    xrarr=xr.DataArray(water_classified_img,name=geometa["name"])

    xrds=xrarr.to_dataset()
    xrds.to_netcdf(path2file)

    return path2file


##############################################################################


if __name__ == "__main__":

    dcdao = DatcubeDao()

    cellindex=(15, -40)
    tile_dat = dcdao.get_nbarpq_data_by_cell_index(cellindex)

    classifier = WaterClassifier()

    icounter = 0
    for (t, nbar, pq) in tile_dat:
    #for (t, nbar, pq) in tile_dat[:3]: #do the first few tiles of the list
        print (t, nbar.shape, pq.shape)
        # print type(nbar), type(pq)

        # (1138232422.2250061, (6, 4000, 4000), (4000, 4000))
        # class 'xarray.core.dataarray.DataArray'>

        # TODO: water classification using these xarray datas ++

        water_classified_img = classifier.classify(nbar.values)


        # 2 Nodata filter
        nodata_val=nbar.attrs["_FillValue"]

        print nodata_val

        water_classified_img = filters.NoDataFilter().apply(water_classified_img, nbar.values, nodata_val)

        print ("Verify the water classified image ")

        # verify that classified image is a 2D (4000X4000) 1 band image with values in defined domain {0,1,..., 128, }

        comput_img_stats(water_classified_img)

        #  save the image to a file: numpy data
        # https://www.google.com.au/webhp?sourceid=chrome-instant&ion=1&espv=2&ie=UTF-8#q=write%20numpy%20ndarray%20to%20file

        outfilename = "waterextent%s.nc" % (icounter)
        path2outf = os.path.join("/g/data1/u46/fxz547/wofs2/extents", outfilename)
        #water_classified_img.tofile(path2outf) #raw data numpy file

        geometadat={"name":"waterextent", "ablersgrid_cellindex": cellindex}
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
