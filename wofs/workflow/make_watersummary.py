#! /bin/env python
"""
Summary all water extent tiles in a directory, to produce a water summary.

"""
import sys, os, time
import matplotlib.pyplot as plt

import numpy as np
import gdal
from gdalconst import *

from wofs.utils.netcdf_io import Netcdf4IO

import logging

# logging.basicConfig(level=logging.INFO)
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)


class SummarizeExtents(object):
    def __init__(self, extentdir, csvfile=None):
        """ extent tiles dir and optionally a csvfile with a list of tiles to be counted for in water summary
        """

        self.extentdir = extentdir
        self.csvfile = csvfile
        self._initialize_observation_counter()

    def _initialize_observation_counter(self, xsize=4000, ysize=4000):
        """ initialize a raster array to count the water observations (pixel v=128)
        and raster array to count the dry obsrvations (pixel value=0)
        """
        self.waterArray = np.zeros((ysize, xsize))
        self.dryArray = np.zeros((ysize, xsize))

        # print type(self.waterArray)
        return

    def _add_obs_layer(self, raster):
        """ add an observationn (which is represented by raster array NxN) into the summary """

        water = raster == 128
        dry = raster == 0

        logging.debug("the raster's water pixels: %s", np.sum(water))
        logging.debug("the raster's dry pixels: %s", np.sum(dry))

        self.waterArray = self.waterArray + water
        self.dryArray = self.dryArray + dry

        return

    def read_extent_file(self, waterextfile):
        """
        read from a water extent file/tile to get water mapping numpy ndarray
        :param waterextfile: path2 file
        :return: 2d numpy array
        """

        if waterextfile.endswith('.tiff') or waterextfile.endswith('.tif'):
            return self.read_geotiff(waterextfile)
        elif waterextfile.endswith('.nc'):
            return self.read_netcdf(waterextfile)
        else:
            raise Exception("Not Implmented to Read File Type for %s " % waterextfile)

    def read_netcdf(self, path2ncfile):
        """
        read from a water extent netcdf file/tile to get the water mapping numpy 2D-darray
        :param path2ncfile:
        :return: 2d numpy array
        """
        from netCDF4 import Dataset, num2date, date2num
        f = Dataset(path2ncfile, 'r')

        # The NC data variable name?
        band4view = 'waterextent'
        bandarray = f[band4view][:, :]  # this will load all data into RAM

        return bandarray

    def read_geotiff(self, geotiff):
        """read a geotiff extent file 1-band
        return a 2D raster array
        """

        # Register drivers
        gdal.AllRegister()

        # Open image
        # ds = gdal.Open('L5102080_08020100109_B10.TIF', GA_ReadOnly)
        ds = gdal.Open(geotiff, GA_ReadOnly)

        if ds is None:
            raise Exception("could not open image %s" % geotiff)

        # get image size
        rows = ds.RasterYSize
        cols = ds.RasterXSize
        numbands = ds.RasterCount

        logging.debug('rows= %s, cols= %s, number of bands = %s', str(rows), str(cols), str(numbands))

        # # get projection and resolution info of the raster
        proj = ds.GetProjection()

        transform = ds.GetGeoTransform()
        xOrigin = transform[0]
        yOrigin = transform[3]
        pixelWidth = transform[1]
        pixelHeight = transform[5]

        # print ("Projection Info = %s"%(proj))
        # print ("xOrigin = %s,  yOrigin = %s "%(xOrigin, yOrigin))
        # print ("pixelWidth = %s,  pixelHeight = %s "%(pixelWidth, pixelHeight))

        # Read the data and do the calculations  

        numarray = []
        for i in range(1, numbands + 1):
            band = ds.GetRasterBand(i)  # the very first band is i=1
            data = band.ReadAsArray(0, 0, cols, rows)  # .astype('float32')
            numarray.append(data)

        return numarray[0]  # only one band for water tiles

    def summarize_extdir(self, fext):
        """ go through every extent tile in the self.extentdir
        """

        import glob

        # filelist= glob.glob(self.extentdir + "/LS8*.tif") #os.listdir(self.extentdir)

        filelist = glob.glob(self.extentdir + "/LS*." + fext)

        logging.info("Number of water extent files to be processed= %s", len(filelist))

        for afile in filelist:
            # print ("processing extent file: " + afile)
            raster = self.read_extent_file(afile)
            # self._add_obs_layer(raster[0])
            self._add_obs_layer(raster)

        return len(filelist)

    def main(self, fext):
        """ main function to call the methods to get water/dry summary
        """

        self.summarize_extdir(fext)

        # now return the final results

        return (self.waterArray, self.dryArray)
        # definition: clearObs = waterObs + dryObs


######################################################################################################
# How to Run:
#   python make_watersummary.py /g/data/u46/wofs/extents/149_-036/ tif
#   python make_watersummary.py /g/data/u46/users/fxz547/wofs2/extents/abc15_-40 nc
#
if __name__ == "__main__":

    if len(sys.argv) < 3:
        print ("USAGE example: python %s /g/data/u46/users/fxz547/wofs2/extents/abc15_-40 [nc|tif]" % sys.argv[0])

        sys.exit(1)

    indir = sys.argv[1]
    file_ext = sys.argv[2]  # file name extension nc or tif

    #derive a ncfile from input info dirname = os.path
    cellid='abc_15_-40'
    cellid=sys.argv[3]

    out_dir='/g/data/u46/users/fxz547/wofs2/fxz547_2016-06-21T14-23-54/summaries'
    outncfile='water_summary_%s.nc'% (cellid)
    path2ncfile=os.path.join(out_dir, outncfile)

    sumObj = SummarizeExtents(indir)

    res = sumObj.main(file_ext)

    print np.sum(res[0])  # water obs
    print np.sum(res[0]+ res[1] )  # res[1] is dry obs 
    # definition?:  clearObs = waterobs + dryobs

    ncobj = Netcdf4IO()

    #  the nc file writer
    mywater_sum = np.empty(dtype='uint8', shape=(2, 4000, 4000))
    mywater_sum[0, :, :] = res[0][:, :]
    mywater_sum[1, :, :] = res[0][:, :] + res[1][:, :]

    metad = {'epoc_seconds': 1234567890.123}

    ncobj.to_file(path2ncfile, mywater_sum, metadict=metad)

    # check the output nc file: ncview, ncdump -hkv

    plt.imshow(res[0])  # water obs, the first band

    # plt.imshow(res[0]+ res[1], cmap='Greys') # Clear observation=0+1 will be the second band

    plt.show()
