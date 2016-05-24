#!/bin/env python
"""
Query AGDC-v2 API to discover input data (NBAR, PQA, DSM) for wofs processing.
input:      config_file
outputs:    cells and their tiles. Directories made

Can re-run. Will converge to the state as specified by the config file.
__author__ = 'fei.zhang@ga.gov.au'
"""

from __future__ import print_function

import os, sys
import csv

from os.path import join as pjoin, dirname, exists
from ConfigParser import ConfigParser
from datetime import datetime
import dateutil.parser
import json
from collections import defaultdict
import xarray as xr
import xarray.ufuncs

from datacube.api import API
from datacube.index import index_connect
from datacube.config import LocalConfig
from datacube.api._conversion import to_datetime
from datacube.api import make_mask

from wofs.workflow.agdc_dao import AgdcDao

from wofs.utils.tools import mkdirs_if_not_present, setup_logging, die, get_config_path_from_args

Satellites = {"LS5": "LANDSAT_5", "LS7": "LANDSAT_7", "LS8": "LANDSAT_8"}

import logging
logging.basicConfig(level=logging.DEBUG)


class WofsQuery:
    """
    Provide a path2configfile to the constructor, parse for the parameter values, and query the agdc for data
    """

    def __init__(self, confile, force_prod=False):
        self.configfile = confile

        self.config = ConfigParser()  # python std lib
        self.config.read(self.configfile)  # got all the data

        self.agdcdao=AgdcDao(force_prod=True)

        # if force_prod:
        #     prod_config = LocalConfig.find(['/g/data/v10/public/modules/agdc-py2-prod/1.0.2/datacube.conf'])
        #     prod_index = index_connect(prod_config, application_name='api-WOfS-prod')
        #     self.dao = API(prod_index)
        # else:
        #     self.dao = API(application_name='api-WOfS')

        return


    def get_query_params(self):
        """
        get the query parameters ready for plugin to AGDC-v2 api
        :return:
        """

        # determine spatial coverage

        lat_range = (
        float(self.config.get('coverage', 'lat_min_deg')), float(self.config.get('coverage', 'lat_max_deg')))

        lon_range = (
        float(self.config.get('coverage', 'lon_min_deg')), float(self.config.get('coverage', 'lon_max_deg')))

        logging.info("Lat range is: %s", lat_range)
        logging.info("Lon range is: %s", lon_range)

        # determine time period

        time_interval = (self.config.get('coverage', 'start_datetime'), self.config.get('coverage', 'end_datetime'))
        # dateutil.parser.parse(self.config.get('coverage','start_datetime')), \
        # dateutil.parser.parse(self.config.get('coverage','end_datetime'))  )

        logging.info(str(time_interval))

        # determine satellite list
        satellites = [Satellites[s] for s in self.config.get('coverage', 'satellites').split(',')]
        logging.info("Satellites: %s", str(satellites))

        # get a CubeQueryContext (this is a wrapper around the API)

        # cube = DatacubeQueryContext()

        # assemble datasets required by a WOfS run

        # dataset_list = [DatasetType.ARG25, DatasetType.PQ25]


        qdict = {"longitude": lon_range, "latitude": lat_range, "time": time_interval, "platform": satellites}

        return qdict


########################################################
    def main(self):
        """
        workflow controller: integrated pipeline
        :return: a state of dirs_files, ready for water-algorithm to run
        """

        # TODO:
        # setup_logging("inputs", "discovery")

        logging.info("Program started")
        inputs_dir = self.config.get('wofs', 'input_dir')
        mkdirs_if_not_present(inputs_dir)

        mkdirs_if_not_present(self.config.get('wofs', 'pyramids_dir'))
        mkdirs_if_not_present(self.config.get('wofs', 'summaries_dir'))

        qdict=self.get_query_params()

        print(qdict)

        cells = self.agdcdao.get_cells_list(qdict)

        # for each cell, create working directories in shadow, sia, extents, bordered_el if they do not exist
        # abc - Australian  AlBers conic Cell index
        for acell in cells:
            cell_id = "abc%s_%s" % (acell)
            mkdirs_if_not_present(pjoin(self.config.get('wofs', 'bordered_elev_tile_path'), cell_id))
            mkdirs_if_not_present(pjoin(self.config.get('wofs', 'tsm_dir'), cell_id))
            mkdirs_if_not_present(pjoin(self.config.get('wofs', 'sia_dir'), cell_id))
            mkdirs_if_not_present(pjoin(self.config.get('wofs', 'extents_dir'), cell_id))


        # Find the tiles for each, and write the tiles reference onto a file in the inputs_dir
        # This prepares for Luigi tasks to classify each tile

        self.agdcdao.get_tiles_for_wofs_inputs(cells, qdict, inputs_dir)


        logging.info("main() Program finished")

        return True  # False if not done completely success


#######################################################


#############################################################################################################
# Usage:
# cd /g/data/u46/fxz547/Githubz/wofs/
# export PYTHONPATH=.:/g/data/u46/fxz547/Githubz/agdc-v2
# python wofs/workflow/wofs_query.py /g/data/u46/fxz547/wofs2/fxz547_2016-05-09T09-54-51/client.cfg
#
#############################################################################################################
if __name__ == '__main__':
    conf_file = sys.argv[1]

    if not os.path.exists(conf_file):
        print ("Error: the input config file %s does not exist"% conf_file)
        sys.exit(2)

    wofsq = WofsQuery(conf_file)

    wofsq.main()
