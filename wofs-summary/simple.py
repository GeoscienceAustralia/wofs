

example_dir = '/g/data/v10/testing_ground/wofs_brl/output/LS8_OLI_WATER/13_-35'
example_file = example_dir + '/LS8_OLI_WATER_3577_13_-35_20130519000352000000_v1502857924.nc'

import numpy as np
import functools

class reader:
    cell = None
    def __init__(self, file):
        self.path = file
        if self.cell is None:
            self.configure_cell(file)
    @classmethod
    def configure_cell(cls, example):
        import rasterio
        with rasterio.open('NetCDF:' + example + ':water') as f:
            cls.cell = dict(affine=f.profile['affine'], crs=f.profile['crs'])
    @property
    def netcdf(self):
        import netCDF4
        return netCDF4.Dataset(self.path)
    @property
    def metadata_doc(self):
        return self.netcdf['dataset'][0].tostring().decode('unicode_escape')
    @property
    @functools.lru_cache()
    def metadata(self):
        # This is slow (almost one second; quarter-second with C version)
        # TODO: consider querying database rather than file.
        # i.e. get JSON, which is faster to parse.
        import yaml
        return yaml.load(self.metadata_doc, Loader=yaml.CLoader)
    @property
    def timestamp(self):
        return self.metadata['extent']['center_dt']
    @property
    def date(self):
        import datetime
        import dateutil.parser
        return (dateutil.parser.parse(self.timestamp) + datetime.timedelta(hours=10)).date()
    @property
    def gqa(self):
        import math
        offset = self.metadata['lineage']['source_datasets']['0'] \
            ['lineage']['source_datasets']['0'] \
            ['lineage']['source_datasets']['level1'] \
            ['gqa']['residual']['iterative_mean']
        return math.hypot(float(offset['x']), float(offset['y']))
        #return self.metadata \
        #    ['lineage']['source_datasets']['0'] \
        #    ['lineage']['source_datasets']['0'] \
        #    ['lineage']['source_datasets']['level1'] \
        #    ['gqa']['residual']['cep90']
    @property
    def water(self):
        return np.squeeze(self.netcdf['water'])

class fuser:
    def __init__(self, tiles):
        self.tiles = tiles
        #print(len(tiles),end='')
    @property
    def water(self):
        output = self.tiles[0].water
        for tile in self.tiles[1:]:
            # TODO: could numexpr
            subsequent = tile.water
            empty = (output & 1).astype(np.bool)
            both = ~empty & ~((subsequent & 1).astype(np.bool))
            output[empty] = subsequent[empty]
            output[both] |= subsequent[both]
        return output

def do_work(observations): # read one file into memory at a time
    import numpy as np

    wet_accumulator = np.zeros((4000,4000),np.uint16)
    dry_accumulator = np.zeros((4000,4000),np.uint16)
    #Could use high and low bits, to increment both variables in one numexpr?

    land_or_sea = ~np.uint8(4) # to mask out the marine flag
    for f in observations:
        bitfield = f.water & land_or_sea
        wet_accumulator += bitfield == 128
        dry_accumulator += bitfield == 0
        #print('.', end='')
    #print('')
    return wet_accumulator, dry_accumulator

def summarise_result(observations, prefix=''):
    wet, dry = do_work(observations)

    clear_observation_count = wet + dry

    import numpy as np
    with np.errstate(invalid='ignore'): # denominator may be zero
        frequency = wet / clear_observation_count

    write(prefix + 'clear.tif', clear_observation_count)
    write(prefix + 'wet.tif', wet)
    write(prefix + 'frequency.tif', frequency.astype(np.float32), nodata=np.nan)

    """
    import matplotlib.pyplot as plt
    fig, (ax1,ax2) = plt.subplots(1,2)
    ax1.imshow(clear_observation_count[::10,::10])
    ax2.imshow(frequency[::10,::10])
    #fig.show()"""

def get_filenames(*directories):
    from glob import glob
    return [x for d in directories for x in glob(d + '/*.nc')]

def get_observations(files):
    import pandas

    rr = map(reader, files)

    p = [(r, r.date) for r in rr if r.gqa<1] # GQA filter threshold
    tiles = pandas.DataFrame(p, columns=['object', 'date'])
    g = tiles.groupby('date', sort=False)

    print('{} observations; {} of {} tiles'.format(len(g), len(p), len(files)))

    return [fuser(list(obs.object)) for date, obs in g]



def show_obs(fus):
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1,1+len(fus.tiles))
    for ax, w in zip(axes, [t.water for t in fus.tiles]+[fus.water]):
        ax.imshow((w & np.uint8(1))[::10,::10])

def write(filename, data, nodata=None):
    import rasterio
    with rasterio.open(filename,
                       mode='w',
                       width=4000,
                       height=4000,
                       count=1,
                       dtype=data.dtype.name,
                       driver='GTIFF',
                       nodata=nodata,
                       tiled=True,
                       compress='DEFLATE',
                       **reader.cell) as destination:
            destination.write(data, 1)

#print(reader(example_file).metadata.keys())

#list(get_observations(example_dir, maxtiles=100))

#summarise_result(get_observations(example_dir))s

#z = get_observations(example_dir)#, maxtiles=5)
#summarise_result(z)


def main():
    import sys
    if len(sys.argv) < 3:
        print("Usage: python simple.py  OUTPUT_PREFIX  INPUT_TILE_DIRS..")
        sys.exit(2)

    output_prefix = sys.argv[1]
    input_dirs = sys.argv[2:]

    obs = get_observations(get_filenames(*input_dirs))
    summarise_result(obs, prefix=output_prefix)

if __name__ == '__main__':
    main()