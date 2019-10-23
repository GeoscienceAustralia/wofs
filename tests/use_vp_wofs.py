from datacube.virtual import Transformation, Measurement, construct, DEFAULT_RESOLVER
import datacube


import yaml

# from fc.fractional_cover import fractional_cover
from wofs.vp_wofs import woffles_ard_no_terrain_filter


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
