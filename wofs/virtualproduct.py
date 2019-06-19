from datacube.virtual import Transformation, Measurement, DEFAULT_RESOLVER
import xarray as xr
from itertools import product

FC_MEASUREMENTS = [{
    'name': 'water',
    'dtype': 'uint8',
    'nodata': 1,
    'units': '1'
}, ]


class Wofs(Transformation):
    """ Applies the wofs algorithm to surface reflectance data.
    Requires bands named 'green', 'red', 'nir', 'swir1', 'swir2'
    """

    def __init__(self, *args, **kwargs):
        self.output_measurements = [Measurement(**m) for m in FC_MEASUREMENTS]

    def measurements(self, input_measurements):
        return self.output_measurements

    def compute(self, data):
        from wofs.vp_wofs import woffles_no_terrain_filter
        sel = [dict(p)
               for p in product(*[[(i.name, i.item()) for i in c]
                                  for v, c in data.coords.items()
                                  if v not in data.geobox.dims])]
        fc = []
        for s in sel:
            fc.append(woffles_no_terrain_filter(data.sel(**s), self.output_measurements))
        return xr.concat(fc, dim='time')


DEFAULT_RESOLVER.lookup_table['transform']['wofs'] = Wofs
