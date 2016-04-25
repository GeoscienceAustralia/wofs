import os, re
from osgeo import gdal,osr
from math import floor,ceil,radians
from eotools.bodies.vincenty import vinc_dist
from eotools.bodies import earth
from gdalconst import *
from numpy import *
from scipy.ndimage.interpolation import rotate
from fileSystem import File

TILE_NAME_PATTERN = 'DSM_(?P<lon>\d{3})_(?P<lat>-\d+)\.tif'
ELEVATION_BAND = 1
SLOPE_BAND = 2
ASPECT_BAND = 3

class Image(object):
    """An abstraction of satellite image data where spectral bands
       are stored as numpy arrays. This class associates no meaning
       to the spectral bands and simply provide basic Image processing
       operation across all bands

       TODO: Refactor so that a Band is a first class object"""

    def __init__(self, bands, metadataDict=None):
        """Construct and image from the supplied data bands 
           
           bands -- a list of ImageBands
        """
        assert type(bands) == list
        self.bands = bands
        self.metadataDict = metadataDict


    def getMetadataDict(self):
        return self.metadataDict

    def setMetadataDict(self, aDict):
        self.metadataDict = aDict

    def getRasterXSize(self):
        return self.bands[0].getRasterXSize()

    def getRasterYSize(self):
        return self.bands[0].getRasterYSize()

    def deleteBand(self, bandNo) :
        assert bandNo < len(self.bands)
        del self.bands[bandNo]

    def getBandCount(self) :
        return len(self.bands)

    def getBand(self, bandNo) :
        assert bandNo < len(self.bands)
        return self.bands[bandNo]

    def appendBand(self, aBand) :
        self.bands.append(aBand)

    def rotate(self, angleDegrees, prefilter=False) :
        """rotate each band by the specified number of degrees
           updating the current image
        """
    
        for bandNo in range(0, len(self.bands)) :
            self.bands[bandNo] = self.bands[bandNo].rotate(angleDegrees, prefilter=prefilter)
         

    def centreCrop(self, sizeTuple) :
        """crop each band to the size specified, original and cropped image share the same centre"""
          
        data = self.bands[0].data
 
        # row crop
        rows = len(data)
        newRows = sizeTuple[0]
        rowStart = (rows-newRows)/2
        rowEnd = rowStart + newRows

        # column crop
        cols = len(data[0])
        newCols = sizeTuple[1]
        colStart = (cols-newCols)/2
        colEnd = colStart + newCols
            
        # create croppedBand
        for band in self.bands:
            band.crop(rowStart, rowEnd, colStart, colEnd)

    def __str__(self):

        return "Image object with %d ImageBands" % len(self.bands)

class ImageBand(object) :
    """A single band in an image"""

    def __init__(self, data, noData=-1000.0) :
        assert len(data.shape) == 2
        self.data = data
        self.noData = noData

    def getData(self):
        return self.data

    def getRasterXSize(self):
        return self.data.shape[1]

   
    def getRasterYSize(self):
        return self.data.shape[0]

    def rotate(self, angleDegrees, prefilter=False, noData=-1000.0) :
        """rotate the band by the specified number of degrees and 
           return the result as a new band. The current image is not
           changed
        """
        # TODO: fix output=float32 below ?? 
        nd = self.noData 
        if nd is None:
            nd = noData 
        newData = rotate(self.data, angleDegrees, 
                                 output=float32, cval=nd, prefilter=prefilter)
        return ImageBand(newData, nd)

    def crop(self, rowStart, rowEnd, colStart, colEnd) :
        self.data = self.data[rowStart:rowEnd, colStart:colEnd]

    def convertTo(self, aTypeName):
        self.data = self.data.astype(aTypeName)

    def getDtype(self): 
        return self.data.dtype

    def getGdalDataType(self):
        instr = str(self.getDtype().name)
        return {
            'uint8'     : 1,
            'uint16'    : 2,
            'int16'     : 3,
            'uint32'    : 4,
            'int32'     : 5,
            'float32'   : 6,
            'float64'   : 7,
            'complex64' : 8,
            'complex64' : 9,
            'complex64' : 10,
            'complex128': 11,
            }.get(instr, 7)


class ImageFile(object) :
    """Abstract class representing an image store in a disk file"""

    def __init__(self, aFile) :
        self.file = aFile
        #self._path = self.file._path #So that self.file.exists() returns true
        self.image = None

class NetCdfImageFile(ImageFile):

    def __init__(self, aNetCdfFile) :
        #assert type(aGeoTifFile) == File
        assert type(aNetCdfFile) == File #MPHQ
        super(NetCdfImageFile, self).__init__(aNetCdfFile)

class GeoTifImageFile(ImageFile):
    """An image stored as a GeoTiff file""" 

    @staticmethod
    def create(aFile, anImage, aGeoTransform=None, aProjection=None, metadataDict=None):
        """Save supplied image to the specified file as a GeoTiff file
        """
        dstOptions = ['COMPRESS=LZW']
        gdal_driver = gdal.GetDriverByName('GTiff')
        bandType = anImage.getBand(0).getGdalDataType()
        dataset = gdal_driver.Create(aFile.getPath(),
                    anImage.getRasterXSize(), anImage.getRasterYSize(),
                    len(anImage.bands), bandType, dstOptions)
        assert dataset, 'Unable to create output dataset %s' % aFile.getPath()
        if aGeoTransform is not None:
            dataset.SetGeoTransform(aGeoTransform)
        if aProjection is not None:
            dataset.SetProjection(aProjection)
        if metadataDict is None:
            metadataDict = anImage.getMetadataDict()
        if metadataDict is not None:
            md = {}
            for key, value in metadataDict.items():
                md[key] = str(value)
            dataset.SetMetadata(md)

        # wrtie each band
        
        for bandNo in range(len(anImage.bands)):
            outputBand = dataset.GetRasterBand(bandNo+1)
            outputBand.WriteArray(anImage.bands[bandNo].data)

        dataset.FlushCache()
        dataset = None

        return GeoTifImageFile(aFile)
        
    def __init__(self, aGeoTifFile) :
        assert type(aGeoTifFile) == File
        super(GeoTifImageFile, self).__init__(aGeoTifFile)

    def loadFile(self) :
        """create the image from the associated file"""
        bands = []
        #print self._path
        assert self.file.exists() == True, 'File %s does not exist' %(self.file.getPath())
        try:
            gdat = gdal.Open(self.file.getPath())
            for bandNo in range(1, gdat.RasterCount+1) :
                raster = gdat.GetRasterBand(bandNo)
                data = raster.ReadAsArray()
                nd = raster.GetNoDataValue()
                band = ImageBand(data, nd)
                bands.append(band)
        finally:
            gdat = None

        self.image = Image(bands)

    def getImage(self) :
        if self.image is None :
            self.loadFile()
        return self.image

    def getProjection(self):
        projection = None
        assert self.file.exists() == True
        try:
            gdat = gdal.Open(self.file.getPath())
            projection = gdat.GetProjection()
        finally:
            gdat = None
        return projection

    def getGeoTransform(self):
        geoTransform = None
        assert self.file.exists() == True
        try:
            gdat = gdal.Open(self.file.getPath())
            geoTransform = gdat.GetGeoTransform()
        finally:
            gdat = None
        return geoTransform

    def getMetadata(self):
        metadata = {}
        assert self.file.exists() == True
        try:
            gdat = gdal.Open(self.file.getPath())
            metadata = gdat.GetMetadata()
        finally:
            gdat = None
        return metadata


    def saveAs(self, destFile) :
        """Save this ImageGeodTifFile to the supplied file"""
        
        dstOptions = ['COMPRESS=LZW']
        gdal_driver = gdal.GetDriverByName('GTiff')
        templateDataset = gdal.Open(self.file.getPath())
        dataset = gdal_driver.Create(destFile.getPath(),
                    self.image.getRasterXSize(), self.image.getRasterYSize(),
                    len(self.image.bands), GDT_Float32, dstOptions)
        assert dataset, 'Unable to create output dataset %s' % filepath
        dataset.SetGeoTransform(templateDataset.GetGeoTransform())
        dataset.SetProjection(templateDataset.GetProjection())

        # wrtie each band
        
        for bandNo in range(len(self.image.bands)):
            outputBand = dataset.GetRasterBand(bandNo+1)
            outputBand.WriteArray(self.image.bands[bandNo].data)
            # outputBand.SetDescription("Base Water band 0=no water, 128=water; Flag bits: b0=nodata, b1=NoContiguity, b2=sea, b3=cloud, b4=cloud shdow, b5=terrain shadow, b6=High slope")
            if self.image.bands[bandNo].noData is not None:
                outputBand.SetNoDataValue(self.image.bands[bandNo].noData)
        dataset.FlushCache()
        dataset = None
        templateDataset = None

