"""
Set individual bitflags needed for wofls.
"""

import constants
import numpy as np
import scipy.ndimage 
import terrain_greg as terrain

def dilate(array, dilation=3):
    """Blocky dilation e.g. for cloud and cloud/terrain shadow"""
    return scipy.ndimage.binary_dilation(array, iterations=dilation, structure=[[1]*3]*3)

def pq_filter(pq):
    """
    Propagate flags from the pixel quality product.

    PQ specs: 16 bits.
        0-7 non-saturation of bands 1-5, 6.1, 6.2, 7. (Note bands 6 are thermal, irrelevent to standard WOfS.)
        8 contiguity (presumably including thermal bands)
        9 land (versus sea)
        10-11 no cloud (ACCA, Fmask)
        12-13 no cloud shadow (ACCA, Fmask)
        14 topographic shadow (not implemented)
        15 unspecified

    Over/under-saturation is flagged in the WOfS journal paper, but may not be previously implemented.

    Notes:
        - will output same flag to indicate noncontiguity, oversaturation and undersaturation.
        - disregarding PQ contiguity flag (see eo_filter instead) to exclude thermal bands.
        - permitting simultaneous flags (through addition syntax) since constants happen to be 
          different powers of the same base.
        - dilates the cloud and cloud shadow. (Previous implementation eroded the negation.)
        - input must be numpy not xarray.DataArray (due to depreciated boolean fancy indexing behaviour)
    """

    PQA_SATURATION_BITS = sum(2**n for n in [0,1,2,3,4,7]) # exclude thermal
    #PQA_CONTIGUITY_BITS = 0x01FF
    PQA_CLOUD_BITS = 0x0C00
    PQA_CLOUD_SHADOW_BITS = 0x3000
    PQA_SEA_WATER_BIT = 0x0200

    masking = np.zeros(pq.shape, dtype=np.uint8) 
    masking[np.logical_not(pq & (PQA_SATURATION_BITS | 0))] = constants.MASKED_NO_CONTIGUITY
    masking[np.logical_not(pq & PQA_SEA_WATER_BIT)] += constants.MASKED_SEA_WATER
    masking[dilate(np.logical_not(pq & PQA_CLOUD_BITS))] += constants.MASKED_CLOUD
    masking[dilate(np.logical_not(pq & PQA_CLOUD_SHADOW_BITS))] += constants.MASKED_CLOUD_SHADOW
    return masking

def terrain_filter(dsm, nbar):
    """
    Terrain shadow masking, slope masking, solar incidence angle masking.

    Input: xarray DataSets
    """

    shadows, slope, sia = terrain.shadows_and_slope(dsm, nbar.blue.time.values)

    shadowy = dilate(shadows != terrain.LIT) | (sia < constants.LOW_SOLAR_INCIDENCE_THRESHOLD_DEGREES)

    steep = (slope > constants.SLOPE_THRESHOLD_DEGREES)

    return np.uint8(constants.MASKED_TERRAIN_SHADOW) * shadowy | np.uint8(constants.MASKED_HIGH_SLOPE) * steep


def eo_filter(source):
    """
    Find where there is no data

    Input must be dataset, not array (since bands could have different nodata values).

    Contiguity can easily be tested either here or using PQ.
    """
    nodata_bools = source.apply(lambda array: array == array.nodata).to_array(dim='band')

    nothingness = nodata_bools.all(dim='band')
    noncontiguous = nodata_bools.any(dim='band')

    return np.uint8(constants.NO_DATA) * nothingness | np.uint8(constants.MASKED_NO_CONTIGUITY) * noncontiguous

