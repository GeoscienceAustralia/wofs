import os
import re
import sys
import numpy
import netCDF4
import rasterio
from glob import glob
from datetime import datetime
from collections import namedtuple
from osgeo import osr





TileInfo = namedtuple('TileInfo', ['filename', 'datetime'])

def parse_filename(filename):
    fields = re.match(
        (
            r"(?P<vehicle>LS[578])"
            r"_(?P<instrument>OLI_TIRS|OLI|TIRS|TM|ETM)"
            r"_(?P<type>WATER)"
            r"_(?P<longitude>[0-9]{3})"
            r"_(?P<latitude>-[0-9]{3})"
            r"_(?P<date>.*)"
            "\.tif$"
        ),
        filename).groupdict()
    return fields
    
def make_tileinfo(filename):
    basename = os.path.basename(filename)
    fields = parse_filename(os.path.basename(basename))
    dt = datetime.strptime(fields['date'][:19], '%Y-%m-%dT%H-%M-%S')
    return TileInfo(filename, datetime=dt)


###############################
def create_netcdf(filename, tiles, zlib_flag=True, timechunksize0=100):

    timechunksize = min(timechunksize0, len(tiles))
    
    # open the first datatset to pull out spatial information
    first = rasterio.open(tiles[0].filename)
    crs = osr.SpatialReference(first.crs_wkt.encode('utf8'))
    affine = first.affine
    width, height = first.width, first.height
        
    with netCDF4.Dataset(filename, 'w') as nco:
        nco.date_created = datetime.today().isoformat()  
        nco.Conventions = 'CF-1.6'  

        # crs variable
        crs_var = nco.createVariable('crs', 'i4')
        crs_var.long_name = crs.GetAttrValue('GEOGCS')
        crs_var.grid_mapping_name = 'latitude_longitude'
        crs_var.longitude_of_prime_meridian = 0.0
        crs_var.spatial_ref = crs.ExportToWkt()
        crs_var.semi_major_axis = crs.GetSemiMajor()
        crs_var.semi_minor_axis = crs.GetSemiMinor()
        crs_var.inverse_flattening = crs.GetInvFlattening()
        crs_var.GeoTransform = affine.to_gdal()

        # latitude coordinate
        nco.createDimension('latitude', height)
        lat_coord = nco.createVariable('latitude', 'float64', ['latitude'])
        lat_coord.standard_name = 'latitude'
        lat_coord.long_name = 'latitude'
        lat_coord.axis = 'Y'
        lat_coord.units = 'degrees_north'
        lat_coord[:] = numpy.arange(height) * affine.e + affine.f + affine.e / 2

        # longitude coordinate
        nco.createDimension('longitude', width)
        lon_coord = nco.createVariable('longitude', 'float64', ['longitude'])
        lon_coord.standard_name = 'longitude'
        lon_coord.long_name = 'longitude'
        lon_coord.axis = 'X'
        lon_coord.units = 'degrees_east'
        lon_coord[:] = numpy.arange(width) * affine.a + affine.c + affine.a / 2

        # time coordinate
        nco.createDimension('time', len(tiles))
        time_coord = nco.createVariable('time', 'double', ['time'])
        time_coord.standard_name = 'time'
        time_coord.long_name = 'Time, unix time-stamp'
        time_coord.axis = 'T'
        time_coord.calendar = 'standard'
        time_coord.units = 'seconds since 1970-01-01 00:00:00'
        time_coord[:] = [(tile.datetime-datetime(1970, 1, 1, 0, 0, 0)).total_seconds() for tile in tiles]

        # wofs data variable
        data_var = nco.createVariable('Data',
                                      'uint8',
                                      ['latitude', 'longitude', 'time'],
                                      chunksizes=[100, 100, timechunksize],
                                      zlib=True)
        data_var.grid_mapping = 'crs'
        data_var.valid_range = [0, 255]
        data_var.flag_masks = [1, 2, 4, 8, 16, 32, 64, 128];
        data_var.flag_meanings = "water cloud cloud_shadow high_slope terrain_shadow over_sea no_contiguity no_source_data"

        tmp = numpy.empty(dtype='uint8', shape=(height, width, timechunksize))
        for start_idx in range(0, len(tiles), timechunksize):
            #read `timechunksize` worth of data into a temporary array
            end_idx = min(start_idx+timechunksize, len(tiles))
            for idx in range(start_idx, end_idx):
                with rasterio.open(tiles[idx].filename) as tile_data:
                    tmp[:,:,idx-start_idx] = tile_data.read(1)
            #write the data into necdffile
            data_var[:,:,start_idx:end_idx] = tmp[:,:,0:end_idx-start_idx]
            sys.stdout.write("\r%d out of %d done" % (end_idx, len(tiles)))
            sys.stdout.flush()

###############################
def create_netcdf_v3(filename, tiles, zlib_flag=True, timechunksize0=100):

    
    timechunksize = min(timechunksize0, len(tiles))
    
    # open the first datatset to pull out spatial information
    first = rasterio.open(tiles[0].filename)
    crs = osr.SpatialReference(first.crs_wkt.encode('utf8'))
    affine = first.affine
    width, height = first.width, first.height
        
    with netCDF4.Dataset(filename, 'w') as nco:
        nco.date_created = datetime.today().isoformat()  
        nco.Conventions = 'CF-1.6'  

        # crs variable
        crs_var = nco.createVariable('crs', 'i4')
        crs_var.long_name = crs.GetAttrValue('GEOGCS')
        crs_var.grid_mapping_name = 'latitude_longitude'
        crs_var.longitude_of_prime_meridian = 0.0
        crs_var.spatial_ref = crs.ExportToWkt()
        crs_var.semi_major_axis = crs.GetSemiMajor()
        crs_var.semi_minor_axis = crs.GetSemiMinor()
        crs_var.inverse_flattening = crs.GetInvFlattening()
        crs_var.GeoTransform = affine.to_gdal()

        # latitude coordinate
        nco.createDimension('latitude', height)
        lat_coord = nco.createVariable('latitude', 'float64', ['latitude'])
        lat_coord.standard_name = 'latitude'
        lat_coord.long_name = 'latitude'
        lat_coord.axis = 'Y'
        lat_coord.units = 'degrees_north'
        lat_coord[:] = numpy.arange(height) * affine.e + affine.f + affine.e / 2

        # longitude coordinate
        nco.createDimension('longitude', width)
        lon_coord = nco.createVariable('longitude', 'float64', ['longitude'])
        lon_coord.standard_name = 'longitude'
        lon_coord.long_name = 'longitude'
        lon_coord.axis = 'X'
        lon_coord.units = 'degrees_east'
        lon_coord[:] = numpy.arange(width) * affine.a + affine.c + affine.a / 2

        # time coordinate
        nco.createDimension('time', len(tiles))
        time_coord = nco.createVariable('time', 'double', ['time'])
        time_coord.standard_name = 'time'
        time_coord.long_name = 'Time, unix time-stamp'
        time_coord.axis = 'T'
        time_coord.calendar = 'standard'
        time_coord.units = 'seconds since 1970-01-01 00:00:00'
        time_coord[:] = [(tile.datetime-datetime(1970, 1, 1, 0, 0, 0)).total_seconds() for tile in tiles]

        # wofs data variable
        data_var = nco.createVariable('Data',
                                      'int8',
                                      ['latitude', 'longitude', 'time'],
                                      #chunksizes=[timechunksize, 100, 100],
                                      chunksizes=[100, 100,timechunksize],
                                      zlib=zlib_flag)
        data_var.grid_mapping = 'crs'
        data_var.valid_range = [0, 255]
        data_var.flag_masks = [1, 2, 4, 8, 16, 32, 64, 128];
        data_var.flag_meanings = "water cloud cloud_shadow high_slope terrain_shadow over_sea no_contiguity no_source_data"

        tmp = numpy.empty(dtype='int8', shape=(height, width,timechunksize))
        for start_idx in range(0, len(tiles), timechunksize):
            #read `timechunksize` worth of data into a temporary array
            end_idx = min(start_idx+timechunksize, len(tiles))
            for idx in range(start_idx, end_idx):
                with rasterio.open(tile.filename) as tile_data:
                    tmp[:,:,idx-start_idx] = tile_data.read(1)
            #write the data into necdffile
            data_var[:,:,start_idx:end_idx] = tmp[:,:,0:end_idx-start_idx]
            sys.stdout.write("\r%d out of %d done" % (end_idx, len(tiles)))
            sys.stdout.flush()
            
###############################
def create_netcdf_v2(filename, tiles, zlib_flag=True, timechunksize=100):

    timechunksize = min(timechunksize, len(tiles))
    
    # open the first datatset to pull out spatial information
    first = rasterio.open(tiles[0].filename)
    crs = osr.SpatialReference(first.crs_wkt.encode('utf8'))
    affine = first.affine
    width, height = first.width, first.height
        
    with netCDF4.Dataset(filename, 'w') as nco:
        nco.date_created = datetime.today().isoformat()  
        nco.Conventions = 'CF-1.6'  

        # crs variable
        crs_var = nco.createVariable('crs', 'i4')
        crs_var.long_name = crs.GetAttrValue('GEOGCS')
        crs_var.grid_mapping_name = 'latitude_longitude'
        crs_var.longitude_of_prime_meridian = 0.0
        crs_var.spatial_ref = crs.ExportToWkt()
        crs_var.semi_major_axis = crs.GetSemiMajor()
        crs_var.semi_minor_axis = crs.GetSemiMinor()
        crs_var.inverse_flattening = crs.GetInvFlattening()
        crs_var.GeoTransform = affine.to_gdal()

        # time coordinate
        nco.createDimension('time', len(tiles))
        time_coord = nco.createVariable('time', 'uint64', ['time'])
        time_coord.standard_name = 'time'
        time_coord.long_name = 'Time, unix time-stamp'
        time_coord.axis = 'T'
        time_coord.calendar = 'standard'
        time_coord.units = 'seconds since 1970-01-01 00:00:00'
        time_coord[:] = [(tile.datetime-datetime(1970, 1, 1, 0, 0, 0)).total_seconds() for tile in tiles]

        # latitude coordinate
        nco.createDimension('latitude', height)
        lat_coord = nco.createVariable('latitude', 'float64', ['latitude'])
        lat_coord.standard_name = 'latitude'
        lat_coord.long_name = 'latitude'
        lat_coord.axis = 'Y'
        lat_coord.units = 'degrees_north'
        lat_coord[:] = numpy.arange(height) * affine.e + affine.f + affine.e / 2

        # longitude coordinate
        nco.createDimension('longitude', width)
        lon_coord = nco.createVariable('longitude', 'float64', ['longitude'])
        lon_coord.standard_name = 'longitude'
        lon_coord.long_name = 'longitude'
        lon_coord.axis = 'X'
        lon_coord.units = 'degrees_east'
        lon_coord[:] = numpy.arange(width) * affine.a + affine.c + affine.a / 2

        # wofs data variable
        data_var = nco.createVariable('Data',
                                      'uint8',
                                      ['time', 'latitude', 'longitude'],
                                      chunksizes=[timechunksize, 100, 100],
                                      zlib=zlib_flag)
        data_var.grid_mapping = 'crs'
        data_var.valid_range = [0, 255]
        data_var.flag_masks = [1, 2, 4, 8, 16, 32, 64, 128];
        data_var.flag_meanings = "water cloud cloud_shadow high_slope terrain_shadow over_sea no_contiguity no_source_data"

        tmp = numpy.empty(dtype='uint8', shape=(timechunksize, height, width))
        for start_idx in range(0, len(tiles), timechunksize):
            #read `timechunksize` worth of data into a temporary array
            end_idx = min(start_idx+timechunksize, len(tiles))
            for idx in range(start_idx, end_idx):
                with rasterio.open(tile.filename) as tile_data:
                    tmp[idx-start_idx] = tile_data.read(1)
            #write the data into necdffile
            data_var[start_idx:end_idx] = tmp[0:end_idx-start_idx]
            sys.stdout.write("\r%d out of %d done" % (end_idx, len(tiles)))
            sys.stdout.flush()
            
###############################
def create_netcdf_v1(filename, tiles, zlib_flag=True, timechunksize=100):
    timechunksize = min(timechunksize, len(tiles))
    
    # open the first datatset to pull out spatial information
    first = rasterio.open(tiles[0].filename)
    affine = first.affine
    width, height = first.width, first.height
        
    with netCDF4.Dataset(filename, 'w') as nco:
        nco.date_created = datetime.today().isoformat()    

        # crs variable
        crs_var = nco.createVariable('crs', 'i4')
        crs_var.long_name = "Lon/Lat Coords in WGS84"
        crs_var.grid_mapping_name = 'latitude_longitude'
        crs_var.longitude_of_prime_meridian = 0.0
        crs_var.spatial_ref = first.crs_wkt
        crs_var.GeoTransform = affine.to_gdal()

        # time coordinate
        nco.createDimension('time', len(tiles))
        time_coord = nco.createVariable('time', 'uint64', ['time'])
        time_coord.standard_name = 'time'
        time_coord.long_name = 'Time, unix time-stamp'
        time_coord.axis = 'T'
        time_coord.calendar = 'standard'
        time_coord.units = 'seconds since 1970-01-01 00:00:00'
        time_coord[:] = [(tile.datetime-datetime(1970, 1, 1, 0, 0, 0)).total_seconds() for tile in tiles]

        # latitude coordinate
        nco.createDimension('latitude', height)
        lat_coord = nco.createVariable('latitude', 'float64', ['latitude'])
        lat_coord.standard_name = 'latitude'
        lat_coord.long_name = 'latitude'
        lat_coord.axis = 'Y'
        lat_coord.units = 'degrees_north'
        lat_coord[:] = numpy.arange(height) * affine.e + affine.f + affine.e / 2

        # longitude coordinate
        nco.createDimension('longitude', width)
        lon_coord = nco.createVariable('longitude', 'float64', ['longitude'])
        lon_coord.standard_name = 'longitude'
        lon_coord.long_name = 'longitude'
        lon_coord.axis = 'X'
        lon_coord.units = 'degrees_east'
        lon_coord[:] = numpy.arange(width) * affine.a + affine.c + affine.a / 2

        # wofs data variable
        data_var = nco.createVariable('wofs',
                                      'uint8',
                                      ['time', 'latitude', 'longitude'],
                                      chunksizes=[timechunksize, 100, 100],
                                      zlib=zlib_flag)
        data_var.grid_mapping = 'crs'
        data_var.valid_range = [0, 255]
        data_var.flag_masks = [1, 2, 4, 8, 16, 32, 64, 128];
        data_var.flag_meanings = "water cloud cloud_shadow high_slope terrain_shadow over_sea no_contiguity no_source_data"

        tmp = numpy.empty(dtype='uint8', shape=(timechunksize, height, width))
        for start_idx in range(0, len(tiles), timechunksize):
            #read `timechunksize` worth of data into a temporary array
            end_idx = min(start_idx+timechunksize, len(tiles))
            for idx in range(start_idx, end_idx):
                with rasterio.open(tile.filename) as tile_data:
                    tmp[idx-start_idx] = tile_data.read(1)
            #write the data into necdffile
            data_var[start_idx:end_idx] = tmp[0:end_idx-start_idx]
            sys.stdout.write("\r%d out of %d done" % (end_idx, len(tiles)))
            sys.stdout.flush()
            




###################################################################

if __name__ == "__main__":
    #extents_dir = '/g/data/u46/wofs/extents/149_-036'
    #extents_dir = '/g/data/u46/fxz547/wofs/extents/149_-036'

    extents_dir = sys.argv[1]

    #zlib_flagv = False 
    zlib_flagv = True 
    ncfilename='stacked_tiffs_zlib_true.nc'

    tiles = [make_tileinfo(filename) for filename in glob(os.path.join(extents_dir, '*.tif'))]
    tiles.sort(key=lambda t: t.datetime)

    output_nc=os.path.join(extents_dir,ncfilename)

    create_netcdf(output_nc, tiles, zlib_flagv)
