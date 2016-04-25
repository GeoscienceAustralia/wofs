import numpy
import numexpr
import rasterio
from math import radians, degrees
from image import *
from wofs.utils.geobox import GriddedGeoBox
import ephem
import logging


SLOPE_BAND=2
ASPECT_BAND=3

logger = logging.getLogger(__name__)

class Sloper(object):
    """
    A Sloper object has the ability to compute the solar incident angle to 
    a given terrain and produce a resulting data band
    """

    def __init__(self, x, y, dsm_path) :
        self.dsm_path = dsm_path
        self.x = x
        self.y = y
        self.geobox = None
        # image = aDsmCell.getImage()
        # self.slope =  image.getBand(SLOPE_BAND).data
        # self.aspect = image.getBand(ASPECT_BAND).data

    def get_geobox(self):
        if self.geobox is None:
            with rasterio.open(self.get_tile_path()) as ds:
                self.geobox = GriddedGeoBox.from_dataset(ds)
        return self.geobox

    def get_tile_path(self):
        return "%s/DSM_%03d_%04d.tif" % (self.dsm_path, self.x, self.y)
        
    def getSunAltAzRadians(self, utc):
        """
        Return the solar alt and az (radians) at the give time over
        the cell associated this shader
        """
        lon, lat = self.get_geobox().centre_lonlat

        observer = ephem.Observer()
        observer.lat = radians(lat)
        observer.lon = radians(lon)
        observer.date = utc
        return  ephem.Sun(observer)


    def get_solar_incident_deg(self, utc):
        """
        Compute the solar incidence angle in degrees from the tangent plane of the slope
        """

        with rasterio.open(self.get_tile_path()) as ds:
            # slope in radians from horizontal plane (derived from the DSM)
            slopeRad    = numpy.deg2rad(ds.read_band(SLOPE_BAND))
        
            # aspect (angle between the projection of slope normal on the
            # horizontal plane and true north measured clockwise) in radians

            aspectRad   = numpy.deg2rad(ds.read_band(ASPECT_BAND))
            self.geobox = GriddedGeoBox.from_dataset(ds)

        # compute the horizontal position of the Sun

        v = self.getSunAltAzRadians(utc)

        # suns azimuth measured clockwise from true north to the sun direction (in degrees)
        solAzDeg = degrees(v.az)
        # suns elevation from the horizontal plane (in degrees)
        solElDeg  = degrees(v.alt)

        logger.debug("Sun AZ: %f deg, Sun ALT: %f deg" % (solAzDeg, solElDeg))


        # get the data we need
    
        # solar azimuth in radians
        solAzRad   = numpy.deg2rad(solAzDeg)
        # solar zenith angle measured in radians
        solElRad  = numpy.deg2rad(solElDeg)

        # cosine of the angle of sunlight incidence with the niormal to the plane of the slope
        # as defined by http://www.powerfromthesun.net/Book/chapter04/chapter04.html
        #
        cosIncidence = numexpr.evaluate( \
            "sin(solElRad) * cos(slopeRad) + (cos(solElRad) * sin(slopeRad) * cos(aspectRad - solAzRad))")
        del solElRad, slopeRad, aspectRad, solAzRad
        logger.debug("Computed cosine of incidence")

        incidenceRad   = numexpr.evaluate("arccos(cosIncidence)")
        del cosIncidence
        incidenceDeg   = numpy.rint(numpy.rad2deg(incidenceRad))
        del incidenceRad

        # compute solar incidence in degrees above plane of the slope
        logger.debug("Creating solarIncidenceDegrees Band")
        band = numexpr.evaluate("90 - incidenceDeg")
        del incidenceDeg

        # clamp negative angles to zero
        logger.debug("Clamping negative angles to zero")
        band = numpy.where(band < 0, 0, band)

        # convert to a signed 8 bit integer 
        logger.debug("Converting to uint8")
        band = band.astype('uint8')

        # construct auxillary data dict

        metadata = {}
        metadata["solar_az_deg"] = solAzDeg
        metadata["solar_elv_deg"] = solElDeg
        metadata["utc"] = utc.isoformat()

        return self.get_geobox(), band, metadata
