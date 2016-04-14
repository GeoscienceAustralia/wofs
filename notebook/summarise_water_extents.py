import sys,os,time
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import numpy as np
import gdal
from gdalconst import *

class SummarizeExtents(object):
    def __init__(self, extentdir, csvfile=None):
        """ extent tiles dir and optionally a csvfile with a list of tiles to be counted for in water summary
        """ 
        
        self.extentdir=extentdir
        self.csvfile=csvfile
        self._initialize_observation_counter()
        
    def _initialize_observation_counter(self,xsize=4000,ysize=4000):
        """ initialize a raster array to count the water observations (pixel v=128)
        and raster array to count the dry obsrvations (pixel value=0)
        """
        self.waterArray = np.zeros((ysize,xsize))
        self.dryArray = np.zeros((ysize,xsize))
        
        print type(self.waterArray)
        return
    
    def _add_obs_layer(self, raster):
        """ add an observationn (which is represented by raster array NxN) into the summary """
        
        water = raster == 128
        dry = raster == 0

        print "the raster's water pixels: ", np.sum(water)
        print "the raster's dry pixels: ", np.sum(dry)

        self.waterArray = self.waterArray + water
        self.dryArray = self.dryArray + dry 
        
        return
    
    def read_extent_file(self, geofile):
        """read a geotiff extent file 1-band
        return a raster array
        """

        # Register drivers
        gdal.AllRegister()

        # Open image
        #ds = gdal.Open('L5102080_08020100109_B10.TIF', GA_ReadOnly)
        ds = gdal.Open(geofile, GA_ReadOnly)

        if ds is None:
            raise Exception("could not open image %s" % geofile)

        # get image size
        rows = ds.RasterYSize
        cols = ds.RasterXSize
        numbands = ds.RasterCount

        print 'rows= %s, cols= %s, number of bands = %s' %(str(rows), str(cols), str(numbands))
        print ("********************")

        # # get projection and resolution info of the raster
        proj = ds.GetProjection()

        transform = ds.GetGeoTransform()
        xOrigin = transform[0]
        yOrigin = transform[3]
        pixelWidth = transform[1]
        pixelHeight = transform[5]

        #print ("Projection Info = %s"%(proj))
        #print ("xOrigin = %s,  yOrigin = %s "%(xOrigin, yOrigin))
        #print ("pixelWidth = %s,  pixelHeight = %s "%(pixelWidth, pixelHeight))

        # Read the data and do the calculations  

        numarray=[]
        for i in range(1,numbands+1):
            band =ds.GetRasterBand(i)  # the very first band is i=1
            data = band.ReadAsArray(0,0,cols,rows) #.astype('float32')
            numarray.append(data)
                            
        return numarray
                            
    def summarize_extdir(self):
        """ go through every extent tile in the self.extentdir
        """

        import glob

        #filelist= glob.glob(self.extentdir + "/LS8*.tif") #os.listdir(self.extentdir)
        filelist= glob.glob(self.extentdir + "/*.tif") #os.listdir(self.extentdir)

        for afile in filelist:
            print ("processing extent file: " + afile)
            raster=self.read_extent_file(afile)
            self._add_obs_layer(raster[0])

        return len(filelist)

    def main(self):
        """ main function to call the methods to get water/dry summary
        """

        self.summarize_extdir()

        # now return the final results

        return (self.waterArray, self.dryArray )
        # definition: clearObs = waterObs + dryObs

if __name__ == "__main__":

    if len(sys.argv)<2:
        print ("USAGE example: python %s /g/data/u46/wofs/extents/130_-012 " %sys.argv[0])  
        sys.exit(1)

    indir = sys.argv[1]
    # infile = sys.argv[2]
    extsumObj = SummarizeExtents( indir )
    
    res = extsumObj.main()

    print np.sum(res[0])  # water obs
    print np.sum(res[1])  # dry obs 
    #definition?:  clearObs = waterobs + dryobs

    #plt.imshow(res[0]) # water obs, the first band
    plt.imshow(res[0]+ res[1], cmap='Greys') # 0+1 should be the clear obs the second band
    plt.show()
