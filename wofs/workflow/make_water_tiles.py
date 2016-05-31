#################################################
# Usage:
#   export PYTHONPATH=/g/data/u46/fxz547/Githubz/wofs/:/g/data/u46/fxz547/Githubz/agdc-v2
#   python make_water_tiles.py
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
from wofs.workflow.agdc_dao import AgdcDao

import numpy
from scipy import stats

import logging

logging.basicConfig(level=logging.INFO)


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


##############################################################################
if __name__ == "__main__":


    # this dict can be created from the wofs user initial input
    qdict={'latitude': (-36.0, -35.0), 'platform': ['LANDSAT_5', 'LANDSAT_7', 'LANDSAT_8'], 'longitude': (149.01, 150.1), 'time': ('2000-01-01', '2016-03-31')}

    #qdict = {'latitude': (-36.0, -35.0), 'platform': ['LANDSAT_8'], 'longitude': (149.01, 150.1), 'time': ('2000-01-01', '2016-03-31')}

    dcdao = AgdcDao()

    cellindex = (15, -40)
    # cellindex = (15, -41)

    tile_data = dcdao.get_nbarpq_data(cellindex, qdict)

    print("Number of (nbar,pqa) tile-pairs:", len(tile_data))

    classifier = WaterClassifier()

    icounter = 0
    #for (t, p, nbar, pq) in tile_data[:2]:  # only process 2-tiles in the list

    for (t, p, nbar, pq) in tile_data:
        print (t, p, nbar.shape, pq.shape)


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

        cellid_str= "%s_%s" % (cellindex)
        celldir="abc%s" % (cellid_str)  # acell's dirname in wofs/extents/

        timestamp=to_datetime(t).isoformat()[:-6]  #remove the trail +00:00, get a str like "2013-04-11T23:46:35.385577"

        print ("DateTime of observation: ", timestamp)

        ts = timestamp.replace(":", "-")
        outfilename = "%s_water_%s_%s.nc" % (p,cellid_str,ts) # LANDSAT_8_water_15_-40_2013-04-11T23:46:35.385577.nc
        path2outf = os.path.join("/g/data1/u46/fxz547/wofs2/extents", celldir, outfilename)
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
#  WOFS-V1
#  Discovery cell.cdv
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

