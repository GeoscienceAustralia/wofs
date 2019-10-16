import xarray as xr
from typing import Dict
from xarray import Dataset
import logging
import numpy

from datacube.testutils.io import dc_read
from datacube.virtual import Transformation, Measurement
from datacube.storage.masking import valid_data_mask
from wofs.vp_wofs import woffles_ard, woffles_ard_no_terrain_filter, fmask_filter, fmask_filter_c2

WOFS_OUTPUT = [{
    'name': 'water',
    'dtype': 'uint8',
    'nodata': 1,
    'units': '1'
}, ]
_LOG = logging.getLogger(__file__)


class WOfSClassifier(Transformation):
    """ Applies the wofs algorithm to surface reflectance data.
    Requires bands named
    bands = ['nbart_blue', 'nbart_green', 'nbart_red', 'nbart_nir', 'nbart_swir_1', 'nbart_swir_2', 'fmask']

    Terrain buffer is specified in CRS Units (typically meters)
    """

    def __init__(self, dsm_path=None, terrain_buffer=0, c2=False):
        self.dsm_path = dsm_path
        self.terrain_buffer = terrain_buffer
        self.c2 = c2
        self.output_measurements = {m['name']: Measurement(**m) for m in WOFS_OUTPUT}
        if dsm_path is None:
            _LOG.warning('WARNING: Path or URL to a DSM is not set. Terrain shadow mask will not be calculated.')

    def measurements(self, input_measurements) -> Dict[str, Measurement]:
        return self.output_measurements

    def compute(self, data) -> Dataset:

        print(data.geobox)
        print(repr(data.geobox))

        masking_used = fmask_filter
        if self.c2:
            masking_used = fmask_filter_c2
            # The C2 data needs to be scaled
            bands = ['nbart_blue', 'nbart_green', 'nbart_red', 'nbart_nir', 'nbart_swir_1', 'nbart_swir_2']

            for band in bands:
                dtype = data[band].dtype
                nodata = data[band].attrs['nodata']
                attrs = data[band].attrs

                is_valid_array = valid_data_mask(data[band])

                data[band] = data[band].where(is_valid_array)
                data[band] = numpy.clip((data[band] * 2.75e-5 - 0.2) * 10000, 0, 10000)
                data[band] = data[band].astype(dtype).where(is_valid_array, nodata)
                data[band].attrs = attrs

        if self.dsm_path is not None:
            dsm = self._load_dsm(data.geobox.buffered(self.terrain_buffer, self.terrain_buffer))

        time_selectors = data.time.values
        wofs = []
        for time in time_selectors:
            if self.dsm_path is None:
                wofs.append(woffles_ard_no_terrain_filter(data.sel(time=time), masking_filter=masking_used
                                                          ).to_dataset(name='water'))
            else:
                wofs.append(woffles_ard(data.sel(time=time), dsm, masking_filter=masking_used
                                        ).to_dataset(name='water'))
        wofs = xr.concat(wofs, dim='time')
        wofs.attrs['crs'] = data.attrs['crs']
        return wofs

    def _load_dsm(self, gbox):
        # Data variable needs to be named elevation
        dsm = dc_read(self.dsm_path, gbox=gbox, resampling="average")
        return xr.Dataset(data_vars={'elevation': (('y', 'x'), dsm)}, coords=_to_xrds_coords(gbox),
                          attrs={'crs': gbox.crs})


def _to_xarray_coords(geobox):
    return tuple((dim, coord.values) for dim, coord in geobox.coordinates.items())


def _to_xrds_coords(geobox):
    return {dim: coord.values for dim, coord in geobox.coordinates.items()}
