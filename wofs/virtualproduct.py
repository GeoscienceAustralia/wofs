import logging
from typing import Dict

import xarray as xr
from datacube.testutils.io import dc_read
from datacube.virtual import Transformation, Measurement
from xarray import Dataset

from wofs.wofls import woffles_ard

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

    def __init__(self, dsm_path=None, terrain_buffer=0):
        self.dsm_path = dsm_path
        self.terrain_buffer = terrain_buffer
        self.output_measurements = {m['name']: Measurement(**m) for m in WOFS_OUTPUT}
        if dsm_path is None:
            # TODO: This should be recorded in the dataset metadata, but we haven't allowed for this
            # in Transformation classes.
            _LOG.warning('WARNING: Path or URL to a DSM is not set. Terrain shadow mask will not be calculated.')

    def measurements(self, input_measurements) -> Dict[str, Measurement]:
        return self.output_measurements

    def compute(self, data) -> Dataset:
        _LOG.info(data.geobox)
        _LOG.info(repr(data.geobox))

        if self.dsm_path is not None:
            dsm = self._load_dsm(data.geobox.buffered(self.terrain_buffer, self.terrain_buffer))
        else:
            dsm = None

        wofs = []
        for time_idx in range(len(data.time)):
            wofs.append(woffles_ard(data.isel(time=time_idx), dsm).to_dataset(name='water'))
        wofs = xr.concat(wofs, dim='time')
        wofs.attrs['crs'] = data.attrs['crs']
        return wofs

    def _load_dsm(self, gbox):
        # Data variable needs to be named elevation
        dsm = dc_read(self.dsm_path, gbox=gbox, resampling="bilinear")
        return xr.Dataset(data_vars={'elevation': (('y', 'x'), dsm)}, coords=_to_xrds_coords(gbox),
                          attrs={'crs': gbox.crs})


def _to_xrds_coords(geobox):
    return {dim: coord.values for dim, coord in geobox.coordinates.items()}

