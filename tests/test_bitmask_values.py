import pytest
from affine import Affine

from datacube.utils.geometry import GeoBox, CRS
from wofs.virtualproduct import WOfSClassifier
import xarray as xr
import numpy as np


def test_nodata_bit_setting(sample_data):
    """

    If no-data bit (bit 1) is set, all other bits should be 0. -- Recommendation from Norman Mueller.
    """

    classifier = WOfSClassifier()

    wofl = classifier.compute(sample_data).water.data.reshape(-1)

    values_with_nodata_bit_set = wofl[np.bitwise_and(wofl, 1) == 1]
    assert values_with_nodata_bit_set == 1


@pytest.fixture
def sample_data():
    required_bands = ['nbart_blue', 'nbart_green', 'nbart_red', 'nbart_nir', 'nbart_swir_1', 'nbart_swir_2', 'fmask']

    return xr.Dataset(
        {
            name: (['time', 'y', 'x'],
                   np.arange(0, 2 ** 16, dtype='uint16').reshape((1, 256, 256)),
                   {'nodata': 0})
            for name in required_bands
        }, attrs={
            'geobox': GeoBox(256, 256, Affine(0.01, 0.0, 139.95,
                                              0.0, -0.01, -49.05), CRS('EPSG:3577')),
            'crs': CRS('EPSG:3577')
        }, coords={
            'time': np.array(['2000-01-01'], dtype='datetime64')
        })
