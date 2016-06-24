# -*- coding: utf-8 -*-
"""
@CreateDate: 2016-06-22

@Author: fei.zhang@ga.gov.au
"""

from __future__ import print_function # make sure print behaves the same in 2.7 and 3.x
import netCDF4     # Note: python is case-sensitive!
import numpy as np

import os, sys
import datetime
import inspect

import logging

# logging.basicConfig(level=logging.INFO)
logging.basicConfig(format='%(asctime)s %(levelname)s - %(message)s', level=logging.DEBUG)


class Netcdf4IO:
    
    def __init__(self, path2ncfile=None, metadict=None):
        
        self.path2ncfile=path2ncfile
        self.metadict= metadict
    
  
    def create_ncfile(self, metadict, dataset_list, output_ncfile):
        """
        create a netcdf file with input metadata dictionary and datasets arrays
        :param metadict:
        :param dataset_list:
        :return: pathto output_ncfile
        """
        # raise Exception ("NotImplementedException")

        output_ncfile= None # if failed to create the required ncfile

        return output_ncfile

        
    def show_vars(self, f, varname):
        """ show info about f varname
        """
        func_name = inspect.stack()[0][3]
        logging.debug("---------------------Function %s(self,f, %s) started -----", func_name, varname)

        if varname not in f.variables.keys():
            logging.debug("The variable %s is not in the netcdf file", varname)
            return False

        ncvar=f.variables[ varname ]


        logging.debug(type(ncvar))

        logging.debug(ncvar)

        #print(ncvar[:])

        # for nitem in range(0,len(ncvar)):
        #     print(ncvar[nitem])

        return True


    def read(self, ncfile):
        """ read from the self.path2ncfile
        """
        func_name = inspect.stack()[0][3]
        logging.debug("---------------------Function %s(self,) started -----", func_name)

        f = netCDF4.Dataset(ncfile)
        #print(f) # heaps of info like ncdump -h
        
# Access a netcdf variables:
#     variable objects stored by name in variables dict.
#     print the variable yields summary info (including all the attributes).
#     no actual data read yet (just have a reference to the variable object with metadata).

        nc_variables= f.variables.keys() # get all variable names
        logging.debug("The netcdf file %s has variables: %s", ncfile, nc_variables)

        for avar in nc_variables:
            self.show_vars(f, avar)

        # self.show_vars(f, 'longitude')
        # self.show_vars(f, 'latitude')
        # self.show_vars(f, 'extra_metadata')

        self.show_vars(f, "nonexistent_haha")

        return

    def get_metad_from_nc(self, ncfile):
        """
        open the datatset to pull out spatial information
        :param ncfile: input ncfile
        :return: a metadata dict
        """

        # in_ncfile = netCDF4.Dataset(ncfile)
        # crs = osr.SpatialReference(in_ncfile.crs_wkt.encode('utf8'))
        # affine = in_ncfile.affine
        # width, height = in_ncfile.width, in_ncfile.height


        mdict=None

        return mdict
    
    def to_file(self, out_ncfile, ndarray, metadict, input_ncfile=None, zlib_flag=True, chuncks=100):
        """
        create a netCDF-4 ncfilename with Data(time,lat,lon) and CF1.6 metadata convention.
        :param in_ncfile: path to an input file from which some geo-metadata can be derived, to be used in out_ncfile
        :param out_ncfile: path to the output file
        :param ndarray: ND array
        :param metadict: metadata dictionary
        :return: the output ptah2filename, or None if unsuccessful.

        Sample: ncdump -h /g/data/u46/users/gxr547/unicube/LS5_TM_NBAR/LS5_TM_NBAR_3577_17_-41_19960403225854000000.nc
        """

        height=4000
        width =4000

        with netCDF4.Dataset(out_ncfile, 'w') as ncobj:
            ncobj.date_created = datetime.datetime.today().isoformat()
            ncobj.Conventions = 'CF-1.6'

            # crs variable
            crs_var = ncobj.createVariable('crs', 'i4')
            crs_var.grid_mapping_name =  'albers_conical_equal_area'
            crs_var.long_name = "GDA94 / Australian Albers";
            crs_var.epsgcode = "EPSG3577"

            # crs_var.long_name = crs.GetAttrValue('GEOGCS')
            # crs_var.spatial_ref = crs.ExportToWkt()
            # crs_var.semi_major_axis = crs.GetSemiMajor()
            # crs_var.semi_minor_axis = crs.GetSemiMinor()
            # crs_var.inverse_flattening = crs.GetInvFlattening()
            # crs_var.GeoTransform = affine.to_gdal()
            

            # # latitude coordinate
            ncobj.createDimension('y', height)
            lat_coord = ncobj.createVariable('y', 'float64', ['y'])
            lat_coord.standard_name = 'projection_y_coordinate'
            lat_coord.long_name = 'latitude'
            lat_coord.axis = 'y coordinate of projection'
            lat_coord.units = 'metre'
            # lat_coord[:] = np.arange(height) * affine.e + affine.f + affine.e / 2
            #
            # # longitude coordinate
            ncobj.createDimension('x', width)
            lon_coord = ncobj.createVariable('x', 'float64', ['x'])
            lon_coord.standard_name = 'projection_x_coordinate'
            lon_coord.long_name = 'x coordinate of projection'
            # lon_coord.axis = 'X'
            lon_coord.units = 'metre'
            # lon_coord[:] = np.arange(width) * affine.a + affine.c + affine.a / 2

            # time coordinate
            ncobj.createDimension('time', 1)
            time_coord = ncobj.createVariable('time', 'double', ['time'])
            time_coord.standard_name = 'epoch time'
            time_coord.long_name = 'Time, unix time-stamp'
            time_coord.axis = 'T'
            time_coord.calendar = 'standard'
            time_coord.units = 'seconds since 1970-01-01 00:00:00'
            time_coord[:] = [metadict.get('epoc_seconds')]

            # wofs data variable
            data4waterobs = ncobj.createVariable('waterobs',
                                            'uint8',  # 'int8',
                                            ['time', 'y', 'x'],
                                            chunksizes=[1, chuncks, chuncks],
                                            zlib=zlib_flag,
                                            complevel=1)  # 1 lest compression, 9 most compression

            data4clearobs = ncobj.createVariable('clearobs',
                                            'uint8',  # 'int8',
                                            ['time', 'y', 'x'],
                                            chunksizes=[1, chuncks, chuncks],
                                            zlib=zlib_flag,
                                            complevel=1)  # 1 lest compression, 9 most compression

            # for the data variables
            # data4waterobs.grid_mapping = 'crs'
            # data4waterobs.value_range = [0, 255]
            # data4waterobs.values = [0, 2, 4, 8, 16, 32, 64, 128];
            # data4waterobs.flag_meanings = "water128 cloud64 cloud_shadow32 high_slope16 terrain_shadow8 over_sea4 no_contiguity2 nodata1 dry0"
            # data4waterobs.dictionary=str(VALUE_DICT)

            # tmp = np.empty(dtype='uint8', shape=(1, height, width))
            # tmp = numpy.empty(dtype='int8', shape=(chunksize, height, width ))


            # write the data into necdffile
            data4waterobs[:,:,:] = ndarray[0,:, :]
            data4clearobs[:,:,:] = ndarray[1,:, :]

            print("done writing %s" % (out_ncfile))

            return out_ncfile


###############################################################################
# Usage Examples:

# python netcdf_io.py /g/data/u46/users/gxr547/unicube/LS5_TM_NBAR/LS5_TM_NBAR_3577_15_-40_19900302231139000000.nc
# python netcdf_io.py /short/public/democube/data/LANDSAT_5_TM_151_-36_PQ_1990-03-02T23-11-04.000000.nc
# --------------------------------------------------------------------
if __name__=="__main__":

    in_ncfile=sys.argv[1]
    
    ncobj=Netcdf4IO()

    # test ncfile reader
    #ncobj.read(in_ncfile)

    # test the nc file writer
    mywater_img= np.empty(dtype='uint8', shape=(2, 4000, 4000))

    metad = {'epoc_seconds':1234567890.123}

    ncobj.to_file('/tmp/ztestfile.nc', mywater_img, metadict=metad)

