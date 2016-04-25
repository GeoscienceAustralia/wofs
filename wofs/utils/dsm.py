import os, re, gdal
from image import GeoTifImageFile
from osgeo import osr
from math import floor,ceil,radians
from eotools.bodies.vincenty import vinc_dist
from wofs.waters.common.datacubeConstants import DatacubeConstants


# from dem_tiler import earth
#https://github.com/GeoscienceAustralia/agdc/blob/2e22c6bdd9305555db3615305ff6a5df6219cd51/deprecated/dem_tiler.py
# WGS-84
earth_A= 6378137.0           # equatorial radius (metres)
earth_B = 6356752.3142        # polar radius (metres)
earth_F = (earth_A - earth_B) /earth_A         # flattening
#ECC2 = 1.0 - B**2/A**2  # squared eccentricity


ELEVATION_BAND = 1
SLOPE_BAND = 2
ASPECT_BAND = 3

class Dsm(object):
    """Represents a Digital Surface Model stored 
       on a file system in a regular grid of cells
       with data store in GeoTif files"""

    def __init__(self, aDirectory):
        """ Create a Dsm object representing the Datacube DSM
            holdings within the supplied datacube directory
        """
        self.directory = aDirectory
        self._load()

    def _load(self):
        cells = {}
        pattern = re.compile(DsmCell.getCellNamePattern())
        for file in self.directory.listFiles() :
            m = pattern.match(file.getName())
            if m is not None :
                cell = DsmCell(self, file, m)
                cells[cell.getKey()] = cell
        self.cells = cells

    def getCell(self, lat, lon):
        """Return the cell referenced by (lat, lon)
           or None if absent
        """
        key = '%03d_%04d' % (lon, lat)
        if key in self.cells :
            return self.cells[key]
        else :
            return None

class DsmCell(object):
    """A single one degree square area within a DSM grid. The 
       digital elevation data is represented by a Image with
       three bands, Elevation, Slope and Aspect"""

    CACHING = True

    @staticmethod
    def set_caching(flag):
        DsmCell.CACHING = flag

    @staticmethod
    def getCellNamePattern() :
        return 'DSM_(?P<lon>\d{3})_(?P<lat>-\d+)\.tif'

    def __init__(self, dsm, aFile, matchObj) :
        """Create the dsm object

           dsm      - the parent Dsm object
           aFile    - the file on disk containing the Dsm data for this cell
           matchObj - a re match object from the filename match
        """

        self.dsm = dsm
        self.lat = int(matchObj.group("lat"))
        self.lon = int(matchObj.group("lon"))
        self.file = aFile
        self.geoTifImageFile = GeoTifImageFile(aFile)

    def __str__(self):
        return "DsmCell: %s at %s" % (self.getKey(), self.getFilePath())

    def getKey(self):
        return "%03d_%04d" % (self.lon, self.lat)
       
    def getFilePath(self):
        return "%s/DSM_%03d_%04d.tif" % (self.dsm.directory.getPath(), self.lon, self.lat)
    
    def getImage(self) :
        image = self.geoTifImageFile.getImage()
        if not DsmCell.CACHING:
            self.dropImage()
        return image
    
    def dropImage(self) :
        """Drop the stored data from the Image object
        Useful if memory is limited
        """
        self.geoTifImageFile.image = None

    def getPixelSize(self, (x, y)=(2000,2000)):
        """
        Returns X & Y sizes in metres of specified pixel as a tuple.
        N.B: Pixel ordinates are zero-based from top left
        Uses the Vincenty computation of pixel scale
        """
        lat = self.lat
        lon = self.lon

        projection = DatacubeConstants.CUBE_PROJECTION    
        spatial_reference = osr.SpatialReference()
        spatial_reference.ImportFromWkt(projection)
        
        geotransform = DatacubeConstants.getGeoTransform(lat, lon)

#    print "\nresults for lat=%f, lon=%f" % (lat,lon)
#    print "geotransform= ", geotransform    
#    print "spatial_reference= ", spatial_reference    
        latlong_spatial_reference = spatial_reference.CloneGeogCS()
        coord_transform_to_latlong = osr.CoordinateTransformation(spatial_reference, latlong_spatial_reference)

        # Determine pixel centre and edges in georeferenced coordinates
        xw = geotransform[0] + x * geotransform[1]
        yn = geotransform[3] + y * geotransform[5] 
        xc = geotransform[0] + (x + 0.5) * geotransform[1]
        yc = geotransform[3] + (y + 0.5) * geotransform[5] 
        xe = geotransform[0] + (x + 1.0) * geotransform[1]
        ys = geotransform[3] + (y + 1.0) * geotransform[5] 
        
    #    print('xw = %f, yn = %f, xc = %f, yc = %f, xe = %f, ys = %f' % (xw, yn, xc, yc, xe, ys))
        
        # Convert georeferenced coordinates to lat/lon for Vincenty
        lon1, lat1, _z = coord_transform_to_latlong.TransformPoint(xw, yc, 0)
        lon2, lat2, _z = coord_transform_to_latlong.TransformPoint(xe, yc, 0)
        #    print('For X size: (lon1, lat1) = (%f, %f), (lon2, lat2) = (%f, %f)' % (lon1, lat1, lon2, lat2))
        x_size, _az_to, _az_from = vinc_dist(earth.F, earth.A, 
                                         radians(lat1), radians(lon1), 
                                         radians(lat2), radians(lon2))
        
        lon1, lat1, _z = coord_transform_to_latlong.TransformPoint(xc, yn, 0)
        lon2, lat2, _z = coord_transform_to_latlong.TransformPoint(xc, ys, 0)
    #    print('For Y size: (lon1, lat1) = (%f, %f), (lon2, lat2) = (%f, %f)' % (lon1, lat1, lon2, lat2))
        y_size, _az_to, _az_from = vinc_dist(earth.F, earth.A, 
                                         radians(lat1), radians(lon1), 
                                         radians(lat2), radians(lon2))
        
        return (x_size, y_size)
