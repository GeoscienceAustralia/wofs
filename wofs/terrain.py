from __future__ import absolute_import, division
import numpy
import ephem
from scipy import ndimage
from pandas import to_datetime
from datacube.utils.geometry import CRS
import math
import xarray

UNKNOWN = -1
LIT = 255
SHADED = 0


def _shade_row(shade_mask, elev_m, sun_alt_deg, pixel_scale_m, no_data, fuzz=0.0):
    """
    shade the supplied row of the elevation model
    """

    # threshold is TAN of sun's altitude
    tan_sun_alt = math.tan(sun_alt_deg)

    # pure terrain angle shadow
    shade_mask[0] = LIT
    shade_mask[1:] = numpy.where((elev_m[:-1] - elev_m[1:]) / pixel_scale_m < tan_sun_alt, LIT, SHADED)

    # project shadows from tips (light->shadow transition)
    switch = numpy.where(shade_mask[:-1] != shade_mask[1:])
    for i in switch[0]:  # note: could use flatnonzero instead of where; or else switch,=; to avoid [0]. --BL
        if shade_mask[i] == LIT:
            # TODO: horizontal fuzz?
            shadow_level = (elev_m[i] + fuzz) - numpy.arange(shade_mask.size - i) * (tan_sun_alt * pixel_scale_m)
            shade_mask[i:][shadow_level > elev_m[i:]] = SHADED

    shade_mask[elev_m == no_data] = UNKNOWN

    return shade_mask


def vector_to_crs(point, vector, original_crs, destination_crs):
    """
        Transform a vector (in the tangent space of a particular point) to a new CRS
        Expects point and vector to each be a 2-tuple in the original CRS.
        Returns a pair of 2-tuples (transformed point and vector).
        Order of coordinates is specified by the CRS (or the OGR library).
    """

    import osr
    # pylint: disable=protected-access,zip-builtin-not-iterating
    transform = osr.CoordinateTransformation(original_crs._crs, destination_crs._crs)
    # theoretically should use infinitesimal displacement
    # i.e. jacobian of the transformation
    # but here just use a finite displatement (for convenience of implementation)
    original_line = [point, tuple(map(sum, zip(point, vector)))]
    transformed_line = [p[:2] for p in transform.TransformPoints(original_line)]  # disregard elevation
    transformed_point = transformed_line[0]
    # take difference (i.e. remove origin offset)
    transformed_vector = tuple(map(lambda x: x[1] - x[0], zip(*transformed_line)))
    return transformed_point, transformed_vector


def solar_vector(p, time, crs):
    (lon, lat), (dlon, dlat) = vector_to_crs(p, (0, 100),
                                             original_crs=crs,
                                             destination_crs=CRS('EPSG:4326'))

    # azimuth north to east of the vertical direction of the crs
    vert_az = math.atan2(dlon * math.cos(math.radians(lat)), dlat)

    observer = ephem.Observer()
    # pylint: disable=assigning-non-slot
    observer.lat = math.radians(lat)
    observer.lon = math.radians(lon)
    observer.date = time
    sun = ephem.Sun(observer)

    sun_az = sun.az - vert_az
    x = math.sin(sun_az) * math.cos(sun.alt)
    y = -math.cos(sun_az) * math.cos(sun.alt)
    z = math.sin(sun.alt)

    return x, y, z, sun_az, sun.alt


# pylint: disable=too-many-locals
def shadows_and_slope(tile, time):
    """
    Terrain shadow masking (Greg's implementation) and slope masking.

    Input: Digital Surface Model xarray DataSet (need metadata e.g. resolution, CRS)

    Uses Sobel filter to estimate the slope gradients (assuming raster is non-rotated wrt. crs) and magnitude.
    Ignores curvature of earth (picking middle of tile for solar elevation and azimuth) calculating surface incidence.
    Reprojects (rotates/resamples) DSM to align rows with shadows (at 25m resolution,
    and assuming the input projection is Mercator-like i.e. preserves bearings).
    For each row, finds each threshold pixel (where the slope just turns away from the sun) and raytraces
    (i.e. using a ramp, masks the other pixels shaded by the pillar of that pixel).
    Reprojects shadow mask (and undoes border enlargement associated with the rotation).

    TODO (BL) -- profile, and explore numpy.minimum.accumulate (make-monotonic) style alternative
                 and maybe fewer resamplings (or come up with something better still).
    """

    y_size, x_size = tile.elevation.shape

    xgrad = ndimage.sobel(tile.elevation, axis=1) / abs(8 * tile.affine.a)
    ygrad = ndimage.sobel(tile.elevation, axis=0) / abs(8 * tile.affine.e)

    # length of the terrain normal vector
    norm_len = numpy.sqrt((xgrad * xgrad) + (ygrad * ygrad) + 1.0)

    # hypot = numpy.hypot(xgrad, ygrad)
    # slope = numpy.degrees(numpy.arctan(hypot))

    slope = numpy.degrees(numpy.arccos(1.0 / norm_len))

    x, y = tile.dims.keys()
    tile_center = (tile[x].values[x_size // 2], tile[y].values[y_size // 2])
    solar_vec = solar_vector(tile_center, to_datetime(time), tile.crs)
    sia = (solar_vec[2] - (xgrad * solar_vec[0]) - (ygrad * solar_vec[1])) / norm_len
    sia = 90 - numpy.degrees(numpy.arccos(sia))

    # # TODO: water_band=SolarTerrainShadowSlope(self.dsm_path).filter(water_band)
    rot_degrees = 90.0 + math.degrees(solar_vec[3])
    sun_alt_deg = math.degrees(solar_vec[4])
    # print solar_vec, rot_degrees, sun_alt_deg
    pixel_scale_m = 25.0  # TODO: proper res
    no_data = -1000

    rotated_elv_array = ndimage.interpolation.rotate(tile.elevation.values,
                                                     rot_degrees,
                                                     reshape=True,
                                                     output=numpy.float32,
                                                     cval=no_data,
                                                     prefilter=False)

    # create the shadow mask by ray-tracying along each row
    shadows = numpy.zeros_like(rotated_elv_array)
    for row in range(0, rotated_elv_array.shape[0]):
        _shade_row(shadows[row], rotated_elv_array[row], solar_vec[4], pixel_scale_m, no_data, fuzz=10.0)

    del rotated_elv_array

    shadows = ndimage.interpolation.rotate(shadows, -rot_degrees, reshape=False, output=numpy.float32, cval=no_data,
                                           prefilter=False)

    dr = (shadows.shape[0] - y_size) // 2
    dc = (shadows.shape[1] - x_size) // 2

    shadows = shadows[dr:dr + y_size, dc:dc + x_size]
    shadows = xarray.DataArray(shadows.reshape(tile.elevation.shape), coords=tile.elevation.coords)

    return shadows, slope, sia
