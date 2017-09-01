

example_dir = '/g/data/v10/testing_ground/wofs_brl/output/LS8_OLI_WATER/13_-35'
example_file = example_dir + '/LS8_OLI_WATER_3577_13_-35_20130519000352000000_v1502857924.nc'

def read_metadata(file):
    import netCDF4
    netcdf = netCDF4.Dataset(file)
    return netcdf['dataset'][0].tostring().decode('unicode_escape')

def get_ortho(file):
    import yaml
    # lineage->source_datasets->classifer
    # Water extent: {(0:nbart -> 0:nbart -> level1:), 1: pqa_wofs, 2: DEM}
    # NBART albers: {0: nbart (scene)}
    return yaml.load(read_metadata(file)) \
        ['lineage']['source_datasets']['0'] \
        ['lineage']['source_datasets']['0'] \
        ['lineage']['source_datasets']['level1'] \
        ['gqa']['residual']['cep90'] # GQA value

def get_water(file):
    import netCDF4
    import numpy as np
    netcdf = netCDF4.Dataset(file)
    return np.squeeze(netcdf['water'])

def do_work(observations): # read one file into memory at a time
    import numpy as np

    wet_accumulator = np.zeros((4000,4000),np.uint16)
    dry_accumulator = np.zeros((4000,4000),np.uint16)
    #Could use high and low bits, to increment both variables in one numexpr?

    land_or_sea = ~np.uint8(4) # to mask out the marine flag
    for f in observations:
        bitfield = get_water(f) & land_or_sea
        wet_accumulator += bitfield == 128
        dry_accumulator += bitfield == 0
        print('.', end='')
    print('')
    return wet_accumulator, dry_accumulator

def produce_result(observations):
    wet, dry = do_work(observations)

    clear_observation_count = wet + dry

    import numpy as np
    with np.errstate(invalid='ignore'): # denominator may be zero
        frequency = wet / clear_observation_count

    import matplotlib.pyplot as plt
    fig, (ax1,ax2) = plt.subplots(1,2)
    ax1.imshow(clear_observation_count[::10,::10])
    ax2.imshow(frequency[::10,::10])
    #fig.show()

import glob
files = glob.glob(example_dir + '/*.nc')

produce_result(files[:10])