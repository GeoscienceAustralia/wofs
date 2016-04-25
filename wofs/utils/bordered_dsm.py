from math import degrees, radians, tan, cos, fabs
import numpy as np
import logging
import rasterio
from wofs import GriddedGeoBox
from scipy.ndimage.interpolation import rotate
import ephem

# shadow mask constants

ELEVATION_BAND = 1
NO_DATA = -1000.0
LIT = 255
SHADED = 0
UNKNOWN = -1

logger = logging.getLogger(__name__)

class BorderedElevationTile(object):
    """
    Elevation data from DSM with a 250 pixel border
    """ 

    def __init__(self, x, y, dsm_dir) :
        """
        x: the longitude of the origin of the central cell
        y: the latitude of the origin of the central cell
        dsm_dir: path to the directory containing DSM data
        """
        self.x = x
        self.y = y
        self.dsm_dir =  dsm_dir

    def _get_tile_path(self, dx, dy):
        """
        Return full path to DSM tile with dx and dy offsets
        """
        return "%s/DSM_%03d_%04d.tif" % (self.dsm_dir, self.x + dx, self.y + dy)

    def get_data(self):
        """
        Construct and return the data array from dsm tiles
        """

        # get the elevation ImageBand 

        logger.debug("creating elevation image for cell %03d_%04d with 250 pixel buffer" \
            % (self.x, self.y))        
        data = np.empty([4500,4500])
        data.fill(NO_DATA)
        geobox = None

        logger.debug("fill central portion of image")
        try:
            with rasterio.open(self._get_tile_path(0, 0)) as src:
                geobox = GriddedGeoBox.from_rio_dataset(src)
                data[250:4250, 250:4250] = src.read_band(ELEVATION_BAND)
        except:
            pass

        try:
            with rasterio.open(self._get_tile_path(-1, 1)) as src:
                logger.debug("NW neighbour")
                data[0:250,0:250] = src.read_band(ELEVATION_BAND, window=((3750,4000), (3750,4000)))
        except:
            pass

        try:
            with rasterio.open(self._get_tile_path(0, 1)) as src:
                logger.debug("N neighbour")
                data[0:250,250:4250] = src.read_band(ELEVATION_BAND, window=((3750,4000), (0,4000)))
        except:
            pass

        try:
            with rasterio.open(self._get_tile_path(1, 1)) as src:
                logger.debug("NE neighbour")
                data[0:250,4250:4500] = src.read_band(ELEVATION_BAND, window=((3750,4000), (0,250)))
        except:
            pass

        try:
            with rasterio.open(self._get_tile_path(-1, 0)) as src:
                logger.debug("W neighbour")
                data[250:4250, 0:250] = src.read_band(ELEVATION_BAND, window=((0,4000), (3750,4000)))
        except:
            pass

        try:
            with rasterio.open(self._get_tile_path(1, 0)) as src:
                logger.debug("E neighbour")
                data[250:4250, 4250:4500] = src.read_band(ELEVATION_BAND, window=((0,4000), (0,250)))
        except:
            pass

        try:
            with rasterio.open(self._get_tile_path(-1, -1)) as src:
                logger.debug("SW neighbour")
                data[4250:4500, 0:250] = src.read_band(ELEVATION_BAND, window=((0,250), (3750,4000)))
        except:
            pass

        try:
            with rasterio.open(self._get_tile_path(0, -1)) as src:
                logger.debug("S neighbour")
                data[4250:4500, 250:4250] = src.read_band(ELEVATION_BAND, window=((0,250), (0,4000)))
        except:
            pass

        try:
            with rasterio.open(self._get_tile_path(1, -1)) as src:
                logger.debug("SE neighbour")
                data[4250:4500, 4250:4500] = src.read_band(ELEVATION_BAND, window=((0,250), (0,250)))
        except:
            pass
  
        # we have data, now adjust the GriddedGeoBox shape and origin to describe the enlarged array

        bigger_geobox = GriddedGeoBox(shape=data.shape, origin=geobox.affine*(-250, -250), \
            pixelsize=geobox.pixelsize, crs=geobox.crs)

        logger.debug("return buffered DSM elevation data")
        return  bigger_geobox, data


def compute_shadows(elev_metres, geobox, utc):
    """
    Compute a shadow mask by raytracing the supplied elevation data at the supplied time

    Parametes: (elev_metres, geobox, utc)
      elev_metres: an array of elevation values in metres (north up)
      geobox: GriddedGeoBox describing the spatial context of the elev_metres array
      utc: datetime (in utc) on which to compute the shadows

    returns: (shadow_mask, geobox, metadata)
      shadow_mask:  A uint8 array, same shape as elev_metres, zero = full sun,
                    non-zero  = in shadow. shadow_mask will be None if the sun is
                    below the horizon
      geobox:       A GriddedGeoBox instance describing the spatial position and extent
                    of the shadow mask. Note that the resulting shadow mask will
                    be larger than the original elev_metres array owing to the impact of
                    rotational reshaping.
      metadata:     A dictionary of useful values:
 
          sun_alt_deg:   The sun's altitude computed for the geobox centre at supplied utc 
          sun_az_deg:    The sun's azimuth  computed for the geobox centre at supplied utc 
          pixel_scale_M: Distance (in metres) across pixel at this utc 
          shadow_mask:   A uint8 array, same shape as elev_metres, zero = full sun,
                         non-zero  = in shadow. shadow_mask will be None if the sun is
                         below the horizon

    """

    # compute solar horizontal positon at centre of geobox, for supplied utc

    observer = ephem.Observer()
    observer.lon = radians(geobox.centre[0])
    observer.lat = radians(geobox.centre[1])
    observer.date = utc
    sun_pos = ephem.Sun(observer)
    sun_alt_deg = degrees(sun_pos.alt)
    sun_az_deg  = degrees(sun_pos.az)

    # compute pixel scale in metres

    (dx, dy) = geobox.get_pixelsize_metres()
    d = dy - dx
    t = fabs(cos(sun_pos.az))
    pixel_scale_M = dx + t*d

    logger.debug("ray tracing shadows for cell %s at %s, sun alt=%f, sun az=%f" \
        % (geobox.origin, utc.isoformat(), sun_alt_deg, sun_az_deg))

    if sun_alt_deg <= 0.0:
        return (sun_alt_deg, sun_az_deg, pixel_scale_M, None)

    # rotate the elevation array so that rows point to the sun
    # This ensures that the ray tracing "walk" down the row 
    # translates to the a unit span across the memory cache 
    # line holding that row.

    rot_degrees = 90.0 + sun_az_deg
    no_data = -1000.0
    logger.debug("rotating elevation band by %f degrees" % (rot_degrees, ))
    rotated_elv_array = rotate(elev_metres, rot_degrees, reshape=True, output=np.float32, cval=no_data, \
        prefilter=False)

    # create the shadow mask by ray-tracying along each row

    shadows = np.zeros_like(rotated_elv_array)
    for row in range(0, rotated_elv_array.shape[0]):
        if row % 400 == 0:
            logger.debug("ray trace row %d" % (row, )) 
        _shadeRow(shadows[row], rotated_elv_array[row], sun_alt_deg, \
            pixel_scale_M, no_data)
    logger.debug("finished ray trace")

    # done with the rotated elevation array, free it's memory

    del rotated_elv_array

    # rotate the shadow mask back to north up orientation

    logger.debug("rotating result band by %f degrees" % (-rot_degrees, ))
    result = rotate(shadows, -rot_degrees, reshape=False, output=np.float32, cval=no_data, \
        prefilter=False)
   
    # assemble metadata

    metadata = { \
        'sun_alt_deg': sun_alt_deg, \
        'sun_az_deg': sun_az_deg, \
        'pixel_scale_M': pixel_scale_M, \
        'utc': utc.isoformat() \
    }

    # compute the new geobox for the enlarged result array

    dr = (result.shape[0] - elev_metres.shape[0]) / 2
    dc = (result.shape[1] - elev_metres.shape[1]) / 2
    new_origin = (-dc, -dr) * geobox.affine

    new_geobox = GriddedGeoBox(shape=result.shape, origin=new_origin, 
        pixelsize=geobox.pixelsize, crs=geobox.crs)
    
    # all done, return result as uint8 array

    logger.debug("returning shadow mask")
    return (result.astype(np.uint8), new_geobox, metadata)


def _shadeRow(shade_mask, elev_M, sun_alt_deg, pixel_scale_M, no_data) :
    """ 
    shade the supplied row of the elevation model
    """

    # threshold is TAN of sun's altitude
    tanSunAlt = tan(radians(sun_alt_deg))

    # look for first pixel with valid elevation
    cols = len(elev_M)
    for i in range(0, cols) :
        if elev_M[i] == no_data :
            shade_mask[i] = UNKNOWN
        else:
            shade_mask[i] = LIT
            break

    # now walk the remainder of the row, setting the mask as we go
    halfPixel = -pixel_scale_M / 2.0
    base = halfPixel
    lastLitHeight = 0.0
    for i in range(i+10, cols) :
        thisHeight = elev_M[i]

        # finish if at a no_data zone
        if thisHeight == no_data:
           break

        heightDiff = lastLitHeight - thisHeight
        base += pixel_scale_M
        if heightDiff <= 0.0 :
            shade_mask[i] = LIT
            lastLitHeight = thisHeight
            base = halfPixel
        else:
            tanTerrain = heightDiff / base
            if tanTerrain < tanSunAlt :
                shade_mask[i] = LIT
                lastLitHeight = thisHeight
                base = halfPixel
            else:
                shade_mask[i] = SHADED

    return shade_mask

