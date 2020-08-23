import pytest
from datetime import datetime
import xarray as xr
from datacube.virtual import Transformation, Measurement, construct, DEFAULT_RESOLVER
import datacube
from pathlib import Path

from wofs.virtualproduct import WOfSClassifier

import yaml

from wofs.vp_wofs import woffles_ard_no_terrain_filter

def test_virtualproduct():
    data = xr.open_dataset(Path(__file__).parent / 'sample_c3_sr.nc', mask_and_scale=False)
    data = data.rename({'oa_fmask': 'fmask'})
    # data = data.expand_dims({'time': [datetime.now()]})
    # data.attrs['crs'] = datacube.utils.geometry.CRS('EPSG:32754')
    data.attrs['crs'] = 'EPSG:32754'

    for dv in data.data_vars.values():
        dv.attrs['nodata'] = dv.attrs['nodatavals']
    # ds['nbart_blue'].attrs['nodata'] = ds['nbart_blue'].attrs['nodatavals']

    transform = WOfSClassifier()
    wofl = transform.compute(data)

    # wofl.to_netcdf(Path(__file__).parent / 'sample_wofl.nc')

    sample = xr.open_dataset(Path(__file__).parent / 'sample_wofl.nc', mask_and_scale=False)

    assert sample.equals(wofl)

def foo():
    # measurements: [green, red, nir, swir1, swir2]
    virtual_product_defn = yaml.safe_load('''
    transform: wofs.virtualproduct.Wofs
    input:
        product: ls8_ard
        measurements: [nbart_blue, nbart_green, nbart_red, nbart_nir, nbart_swir_1, nbart_swir_2, fmask]
    ''')
    virtual_product = construct(**virtual_product_defn)

    # [odc_conf_test] -
    # db_hostname: agdcdev-db.nci.org.au
    # db_port: 6432
    # db_database: odc_conf_test


    # [ard_interop] - collection upgrade DB
    # db_hostname: agdcstaging-db.nci.org.au
    # db_port:     6432
    # db_database: ard_interop

    dc = datacube.Datacube(env="odc_conf_test")


    vdbag = virtual_product.query(dc=dc, id='be43b7ce-c421-4c16-826d-a508f3e3d984')

    box = virtual_product.group(vdbag, output_crs='EPSG:28355', resolution=(-25, 25))

    virtual_product.output_measurements(vdbag.product_definitions)

    data = virtual_product.fetch(box, dask_chunks=dict(x=1000, y=1000))

    print(data)

    # crash!
    # done = data.compute()
