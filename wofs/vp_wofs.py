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


import numpy as np

import wofs.classifier
# import wofs.terrain
from wofs.constants import NO_DATA, MASKED_CLOUD, MASKED_CLOUD_SHADOW
from wofs.filters import eo_filter, terrain_filter
from datacube.storage import masking


def fmask_filter(fmask):
    masking = np.zeros(fmask.shape, dtype=np.uint8)
    masking[fmask == 0] += NO_DATA
    masking[fmask == 2] += MASKED_CLOUD
    masking[fmask == 3] += MASKED_CLOUD_SHADOW

    return masking


def fmask_filter_c2(fmask):
    mask = np.zeros(fmask.shape, dtype=np.uint8)
    col2_nodata = masking.make_mask(fmask, nodata=True)
    col2_cloud = masking.make_mask(fmask, cloud_or_cirrus='cloud_or_cirrus')
    col2_cloud_shadow = masking.make_mask(fmask, cloud_shadow='cloud_shadow')

    mask[col2_cloud.values] += MASKED_CLOUD
    mask[col2_cloud_shadow.values] += MASKED_CLOUD_SHADOW
    mask[col2_nodata.values] = NO_DATA
    return mask


def classify_ard(ds):
    """Put the bands in the expected order, and exclude the fmask band, then classify"""
    bands = ['nbart_blue', 'nbart_green', 'nbart_red', 'nbart_nir', 'nbart_swir_1', 'nbart_swir_2']
    return wofs.classifier.classify(ds[bands].to_array(dim='band'))


def woffles_ard_no_terrain_filter(ard, masking_filter=fmask_filter):
    """Generate a Water Observation Feature Layer from ARD (NBART and FMASK) surface elevation inputs."""

    water = classify_ard(ard) | eo_filter(ard) | masking_filter(ard.fmask)

    assert water.dtype == np.uint8

    return water


def woffles_ard(ard, dsm, masking_filter=fmask_filter):
    water = classify_ard(ard) | eo_filter(ard) | masking_filter(ard.fmask) | terrain_filter(dsm, ard.rename(
        {'nbart_blue': 'blue'}))

    assert water.dtype == np.uint8

    return water
