#!/bin/env python
"""
Query AGDC-v2 API to discover input data (NBAR, PQA, DSM) for wofs processing.
input:      config_file
outputs:    cells and their tiles. Directories made

Can re-run. Will converge to the state as specified by the config file.
__author__ = 'fzhang'
"""

import os, sys
import csv
from os.path import join as pjoin, dirname, exists

import logging
# from wofs import mkdirs_if_not_present, setup_logging, die, get_config_path_from_args
from wofs.utils.tools import mkdirs_if_not_present, setup_logging, die, get_config_path_from_args

from ConfigParser import ConfigParser

from datetime import datetime
# from wofs import TimeInterval, DatacubeQueryContext, WofsConfig

import dateutil.parser
import json

from collections import defaultdict

import xarray as xr
import xarray.ufuncs

from datacube.api import API
from datacube.index import index_connect
from datacube.config import LocalConfig
from datacube.api._conversion import to_datetime
from datacube.api import make_mask, describe_flags

Satellites = {"LS5": "LANDSAT_5", "LS7": "LANDSAT_7", "LS8": "LANDSAT_8"}

logging.basicConfig(level=logging.DEBUG)


class WofsQuery:
    """
    Provide a path2configfile to the constructor, parse for the parameter values, and query the agdc for data
    """

    def __init__(self, confile, force_prod=False):
        self.configfile = confile

        self.config = ConfigParser()  # python std lib
        self.config.read(self.configfile)  # got all the data

        if force_prod:
            prod_config = LocalConfig.find(['/g/data/v10/public/modules/agdc-py2-prod/1.0.2/datacube.conf'])
            prod_index = index_connect(prod_config, application_name='api-WOfS-prod')
            self.dao = API(prod_index)
        else:
            self.dao = API(application_name='api-WOfS')

        return


    def get_query_params(self):
        """
        derive the query parameters ready for plugin to AGDC-v2 api
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


    def get_cells_list(self):
        """
        Get a List of all relevant cell indexes:
        cells = dc.list_cells(product='nbar', longitude=lon_range, latitude=lat_range, time=time_interval, platform=satellites)

        all NBAR cells dc.list_cells(product='nbar')

        dc.list_cells(product='nbar', longitude=149, latitude=-35, time=('1990', '2015'))
        :return:
        """

        dc = self.dao # API(app='WOfS-dev')

        # Query parameters:

        qdict = self.get_query_params()

        logging.debug(qdict)

        #{"product": "nbar", "longitude": (149, 150), "latitude": (-40, -41), "time": ('1900-01-01', '2016-03-20')}

        cells = dc.list_cells(**qdict)

        logging.info(str(cells))

        #return cells_filtered_list =filter_cell_list(cells)
        return cells


    def get_tiles_by_cell(self, acell):
        """
        query to discover all relevant tiles for a given cell_Id
        :return:
        """

        tile_store = self.get_tile_store([acell])

        for cell, stack in tile_store.items():
            for time in sorted(stack):
                print ("time=", time)

                tiles = stack[time]

                print (cell, time, len(tiles)), type(tiles)

                print "Cell {} at time [{:%Y-%m-%d}] has {} tiles: ".format(cell, to_datetime(time), len(tiles))
                for product, tile in tiles.items():
                    print product, tile

    def get_tile_store(self, cells):
        """
        return a dictionary of tiles
        :param cells:
        :return:
        """

        # eg, cells = [(15, -40)]  #cover Canberra

        dc = self.dao

        nbar_tiles = dc.list_tiles(cells, product='nbar', platform='LANDSAT_8')  # ,time=('2000', '2007'))
        pq_tiles = dc.list_tiles(cells, product='pqa', platform='LANDSAT_8')  # , time=('2000', '2007'))

        if (len(pq_tiles) == len(nbar_tiles)):
            print ("The cell %s has %s nbar and %s pq tiles" % (str(cells), len(nbar_tiles), len(pq_tiles)))
        else:
            print "WARNING: unequal number of nbar and pq tiles: "
            print ("The cell %s has %s nbar and %s pq tiles" % (str(cells), len(nbar_tiles), len(pq_tiles)))

        tile_store = defaultdict(lambda: defaultdict(dict))

        for tile_query, tile_info in nbar_tiles:
            cell = tile_query['xy_index']
            time = tile_query['time']
            product = tile_info['metadata']['product_type']
            tile_store[cell][time][product] = (tile_query, tile_info)

        for tile_query, tile_info in pq_tiles:
            cell = tile_query['xy_index']
            time = tile_query['time']
            product = tile_info['metadata']['product_type']
            tile_store[cell][time][product] = (tile_query, tile_info)

        return tile_store


    def get_tile_store2(self, cells):
        """
        return a dictionary of tiles
        :param cells:
        :return:
        """
        dc = API(app='WOfS-dev')
        # eg, cells = [(15, -40)]  #cover Canberra

        nbar_tiles = dc.list_tiles(cells, product='nbar',
                                   platform=['LANDSAT_5', 'LANDSAT_8'])  # ,time=('2000', '2007'))
        pq_tiles = dc.list_tiles(cells, product='pqa', platform=['LANDSAT_5', 'LANDSAT_8'])  # , time=('2000', '2007'))

        if (len(pq_tiles) == len(nbar_tiles)):
            print ("The cells %s has %s nbar and %s pq tiles" % (str(cells), len(nbar_tiles), len(pq_tiles)))
        else:
            print "WARNING: unequal number of nbar and pq tiles: "
            print ("The cells %s has %s nbar and %s pq tiles" % (str(cells), len(nbar_tiles), len(pq_tiles)))

        tile_store = defaultdict(lambda: defaultdict(dict))

        for tile_query, tile_info in nbar_tiles:
            cell = tile_query['xy_index']
            time = tile_query['time']
            product = tile_info['metadata']['product_type']
            tile_store[cell][time][product] = (tile_query, tile_info)

        for tile_query, tile_info in pq_tiles:
            cell = tile_query['xy_index']
            time = tile_query['time']
            product = tile_info['metadata']['product_type']
            tile_store[cell][time][product] = (tile_query, tile_info)

        return tile_store

    def old_fun_include_exclude_logics(self):
        """
        includes- and excludes- filtering of the cells  discovered by the  agdc-api query.
        :return: filtered-cells-list, which is finalized for processing
        """
        # filter-cells to be processed

        inputs_dir = self.config.get('wofs', 'input_dir')
        mkdirs_if_not_present(inputs_dir)

        tiles_csv_path = "%s/tiles.csv" % (inputs_dir,)

        # now query the datacube to create the tiles.csv file

        tiles = cube.tile_list_to_file(lon_range, lat_range, satellites, time_interval, \
                                       dataset_list, tiles_csv_path)
        logging.info("%s created" % (tiles_csv_path,))

        # sort that file by lat/lon
        # TODO: The following UNIX sort stage will not be necessary if
        # when the API offers to sort the tile list by cell_id

        logging.info("sorting %s in cell ID order" % (tiles_csv_path,))
        sorted_tiles_csv_path = "%s/sorted_tiles.csv" % (inputs_dir,)
        sort_csv_file(tiles_csv_path, sorted_tiles_csv_path, (7, 8))
        logging.info("%s created" % (sorted_tiles_csv_path,))

        # create a list of cells to include

        include_cells = None
        cell_list = self.config.get('coverage', 'include_cells', None)
        if cell_list is not None and len(cell_list) > 0:
            include_cells = json.loads(cell_list)

        # create a list of cells to exclude

        exclude_cells = None
        cell_list = self.config.get('coverage', 'exclude_cells', None)
        if cell_list is not None and len(cell_list) > 0:
            exclude_cells = json.loads(cell_list)

        # split the tile csv file into cell csv files (all tiles from one cell
        # in one csv file

        last_xy = None
        cell_ids = []
        with open(sorted_tiles_csv_path, "rb") as f:
            reader = csv.DictReader(f)
            fname = None
            all_tile_count = 0
            tile_count = 0
            for record in reader:
                tile = Tile.from_csv_record(record)

                # specific inclusions

                if include_cells is not None and \
                                list(tile.xy) not in include_cells:
                    continue

                # specific exclusions

                if exclude_cells is not None and \
                                list(tile.xy) in exclude_cells:
                    continue

                if tile.xy != last_xy:
                    if fname is not None:
                        logging.info("created %s with %d tiles" % (fname, tile_count))
                    cell_id = "%03d_%04d" % (tile.xy[0], tile.xy[1])
                    fname = "%s/cell_%s_tiles.csv" % (inputs_dir, cell_id)
                    writer = csv.DictWriter(open(fname, "wb"), reader.fieldnames)
                    writer.writeheader()
                    last_xy = tile.xy
                    cell_ids.append(cell_id)
                    all_tile_count += tile_count
                    tile_count = 0

                writer.writerow(record)
                tile_count += 1
            logging.info("created %s with %d tiles" % (fname, tile_count))
            logging.info("%d tiles discovered" % (tile_count + all_tile_count,))



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

        self.get_query_params()

        cells = self.get_cells_list()

        # create working directories if they do not exist
        # create dirs according to cell index
        for acell in cells:
            cell_id = "abc%s_%s" % (acell)  # abc - Australian  AlBers conic Cell index
            mkdirs_if_not_present(pjoin(self.config.get('wofs', 'bordered_elev_tile_path'), cell_id))
            mkdirs_if_not_present(pjoin(self.config.get('wofs', 'tsm_dir'), cell_id))
            mkdirs_if_not_present(pjoin(self.config.get('wofs', 'sia_dir'), cell_id))
            mkdirs_if_not_present(pjoin(self.config.get('wofs', 'extents_dir'), cell_id))


        # self.get_tile_store(cells)

        for acell in cells:
            self.get_tiles_by_cell(acell)


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

    wofsq = WofsQuery(conf_file)

    wofsq.main()
