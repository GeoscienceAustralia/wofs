import xarray as xr
from typing import Dict
from xarray import Dataset

from datacube.testutils.io import dc_read
from datacube.virtual import Transformation, Measurement
from wofs.vp_wofs import woffles_ard

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

    def __init__(self, dsm_path, terrain_buffer=0):
        self.dsm_path = dsm_path
        self.terrain_buffer = terrain_buffer
        self.output_measurements = {m['name']: Measurement(**m) for m in WOFS_OUTPUT}

    def measurements(self, input_measurements) -> Dict[str, Measurement]:
        return self.output_measurements

    def compute(self, data) -> Dataset:
        print(data.geobox)
        print(repr(data.geobox))
        dsm = self._load_dsm(data.geobox.buffered(self.terrain_buffer, self.terrain_buffer))

        time_selectors = data.time.values
        wofs = []
        for time in time_selectors:
            wofs.append(woffles_ard(data.sel(time=time), dsm).to_dataset(name='water'))
        wofs = xr.concat(wofs, dim='time')
        wofs.attrs['crs'] = data.attrs['crs']
        return wofs

    def _load_dsm(self, gbox):
        # Data variable needs to be named elevation
        dsm = dc_read(self.dsm_path, gbox=gbox)
        return xr.Dataset(data_vars={'elevation': (('y', 'x'), dsm)}, coords=_to_xrds_coords(gbox),
                          attrs={'crs': gbox.crs})


def _to_xarray_coords(geobox):
    return tuple((dim, coord.values) for dim, coord in geobox.coordinates.items())


def _to_xrds_coords(geobox):
    return {dim: coord.values for dim, coord in geobox.coordinates.items()}
