# For the given area
#   load the new CU data in native projection
#   classify using WOfS classifier
#   mask using fmask / dsm
#   load current WOfS data - in same projection as CU
#   compare two classifications
#   save out comparison file with bands:
#     'new_water'
#     'curr_water'
#     'missing'
#     'extra'
#     'match'


import datacube
import wofs.wofs_app

import numpy as np
import scipy
import xarray

MASKED_HIGH_SLOPE = 1 << 4   # (dec 16)  bit 4: 1=pixel masked out due to high slope
MASKED_TERRAIN_SHADOW = 1 << 3  # (dec 8)   bit 3: 1=pixel masked out due to terrain shadow or
MASKED_NO_CONTIGUITY = 1 << 1   # (dec 2)   bit 1: 1=pixel masked out due to lack of data contiguity
NO_DATA = 1 << 0   # (dec 1)   bit 0: 1=pixel masked out due to NO_DATA in NBAR source, 0=valid data in NBAR
MASKED_CLOUD = 1 << 6   # (dec 64)  bit 6: 1=pixel masked out due to cloud
MASKED_CLOUD_SHADOW = 1 << 5   # (dec 32)  bit 5: 1=pixel masked out due to cloud shadow

# Water detected on slopes equal or greater than this value are masked out
SLOPE_THRESHOLD_DEGREES = 12.0
# If the sun only grazes a hillface, observation unreliable (vegetation shadows etc)
LOW_SOLAR_INCIDENCE_THRESHOLD_DEGREES = 30



#--------------These are from wofs and should be referenced there rather than copied------
def eo_filter(source):
    """
    Find where there is no data
    Input must be dataset, not array (since bands could have different nodata values).
    """
    nodata_bools = source.apply(lambda array: array == array.nodata).to_array(dim='band')

    nothingness = nodata_bools.all(dim='band')
    noncontiguous = nodata_bools.any(dim='band')

    return np.uint8(NO_DATA) * nothingness | np.uint8(MASKED_NO_CONTIGUITY) * noncontiguous


def dilate(array):
    """Dilation e.g. for cloud and cloud/terrain shadow"""
    # kernel = [[1] * 7] * 7 # blocky 3-pixel dilation
    y, x = np.ogrid[-3:4, -3:4]
    kernel = ((x * x) + (y * y) <= 3.5**2)  # disk-like 3-pixel radial dilation
    return scipy.ndimage.binary_dilation(array, structure=kernel)


def terrain_filter(dsm, nbar):
    """
        Terrain shadow masking, slope masking, solar incidence angle masking.
        Input: xarray DataSets
    """

    shadows, slope, sia = wofs.terrain.shadows_and_slope(dsm, nbar.nbart_blue.time.values)

    shadowy = dilate(shadows != wofs.terrain.LIT) | (sia < LOW_SOLAR_INCIDENCE_THRESHOLD_DEGREES)

    steep = (slope > SLOPE_THRESHOLD_DEGREES)

    result = np.uint8(MASKED_TERRAIN_SHADOW) * shadowy | np.uint8(MASKED_HIGH_SLOPE) * steep

    return xarray.DataArray(result, coords=[dsm.y, dsm.x])  # note, assumes (y,x) axis ordering


#-------------------------------------------------------------------------------------------
def pq_filter(fmask):
    masking = np.zeros(fmask.shape, dtype=np.uint8)
    masking[fmask == 0] += NO_DATA
    masking[fmask == 2] += MASKED_CLOUD
    masking[fmask == 3] += MASKED_CLOUD_SHADOW

    return masking


def classify(ds):
    bands = ['nbart_blue', 'nbart_green', 'nbart_red', 'nbart_nir', 'nbart_swir_1', 'nbart_swir_2']
    return wofs.classifier.classify(ds[bands].to_array(dim='band'))


def woffles_no_terrain_filter(source):
    """Generate a Water Observation Feature Layer from NBAR, PQ and surface elevation inputs."""

    water = classify(source) | eo_filter(source) | pq_filter(source.fmask)

    assert water.dtype == np.uint8

    return water
