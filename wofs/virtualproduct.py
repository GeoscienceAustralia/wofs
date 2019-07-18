from collections import Mapping
from typing import Dict

import xarray as xr
from itertools import product
from xarray import Dataset

from datacube.virtual import Transformation, Measurement

WOFS_OUTPUT = [{
    'name': 'water',
    'dtype': 'uint8',
    'nodata': 1,
    'units': '1'
}, ]


class WOfSClassifier(Transformation):
    """ Applies the wofs algorithm to surface reflectance data.
    Requires bands named
    bands = ['nbart_blue', 'nbart_green', 'nbart_red', 'nbart_nir', 'nbart_swir_1', 'nbart_swir_2', 'fmask']
    """

    def __init__(self, *args, **kwargs):
        self.output_measurements = {m['name']: Measurement(**m) for m in WOFS_OUTPUT}

    def measurements(self, input_measurements) -> Dict[str, Measurement]:
        return self.output_measurements

    def compute(self, data) -> Dataset:
        from wofs.vp_wofs import woffles_no_terrain_filter
        time_selectors = data.time.values
        wofs = []
        for time in time_selectors:
            wofs.append(woffles_no_terrain_filter(data.sel(time=time)).to_dataset(name='wofs', dim='water'))
        wofs = xr.concat(wofs, dim='time')
        wofs.attrs['crs'] = data.attrs['crs']
        return wofs
