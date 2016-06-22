# -*- coding: utf-8 -*-
"""
@CreateDate: 2016-06-22

@Author: fei.zhang@ga.gov.au
"""

from __future__ import print_function # make sure print behaves the same in 2.7 and 3.x
import netCDF4     # Note: python is case-sensitive!
import numpy as np

import os, sys
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

        for nitem in range(0,len(ncvar)):
            print(ncvar[nitem])

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

        self.show_vars(f, 'time')
        # self.show_vars(f, 'longitude')
        # self.show_vars(f, 'latitude')
        self.show_vars(f, 'extra_metadata')

        return
###############################################################################
# Usage Examples:

# python netcdf_io.py /g/data/u46/users/gxr547/unicube/LS5_TM_NBAR/LS5_TM_NBAR_3577_15_-40_19900302231139000000.nc
# python netcdf_io.py /short/public/democube/data/LANDSAT_5_TM_151_-36_PQ_1990-03-02T23-11-04.000000.nc
# --------------------------------------------------------------------
if __name__=="__main__":

    in_ncfile=sys.argv[1]
    
    ncobj=Netcdf4IO()
    ncobj.read(in_ncfile)

