from datacube.virtual import Transformation, Measurement, DEFAULT_RESOLVER
import xarray as xr
from itertools import product

WOFS_OUTPUT = [{
    'name': 'water',
    'dtype': 'uint8',
    'nodata': 1,
    'units': '1'
}, ]


class Wofs(Transformation):
    """ Applies the wofs algorithm to surface reflectance data.
    Requires bands named
    bands = ['nbart_blue', 'nbart_green', 'nbart_red', 'nbart_nir', 'nbart_swir_1', 'nbart_swir_2', 'fmask']
    """

    def __init__(self, *args, **kwargs):
        self.output_measurements = [Measurement(**m) for m in WOFS_OUTPUT]

    def measurements(self, input_measurements):
        return self.output_measurements

    def compute(self, data):
        from wofs.vp_wofs import woffles_no_terrain_filter
        sel = [dict(p)
               for p in product(*[[(i.name, i.item()) for i in c]
                                  for v, c in data.coords.items()
                                  if v not in data.geobox.dims])]
        wofs = []
        for s in sel:
            wofs.append(woffles_no_terrain_filter(data.sel(**s)))
        return xr.concat(wofs, dim='time')


DEFAULT_RESOLVER.lookup_table['transform']['wofs'] = Wofs
