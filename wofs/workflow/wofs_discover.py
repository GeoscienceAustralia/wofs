#!/bin/env python
"""
Enquire against the AGDC API and prepare inputs for a WOfS run 
"""

import os
import csv
from os.path import join as pjoin, dirname, exists
import argparse
import logging
#from wofs import mkdirs_if_not_present, setup_logging, die, get_config_path_from_args
from wofs.utils import mkdirs_if_not_present, setup_logging, die, get_config_path_from_args
import luigi
from datacube.api.model import DatasetType, Tile, Cell, Satellite
from datetime import datetime
#from wofs import TimeInterval, DatacubeQueryContext, WofsConfig
from wofs.agdc_wrapper import TimeInterval, DatacubeQueryContext
from wofs.config import WofsConfig
import dateutil.parser
import json


CONFIG = luigi.configuration.get_config()

def sort_csv_file(in_csv_file, out_csv_file, keys, delimiter=','):
    """
    Perform a UNIX sort on the supplied in_csv_file, writing the result
    to the  out_csv_file. Keys is a list of csv field numbers (first field is 1)
    that are to be used for sorting. The input CSV file is assumed to contain
    column headings.
    """
    cmd = \
        "head -n 1 %s > %s\n" % (in_csv_file, out_csv_file) + \
        "tail --lines +2 %s | sort --field-separator=',' --key=%s >> %s" \
        % (in_csv_file, ",".join(map(str,keys)), out_csv_file)

    os.system(cmd)

def main():

    # get config path from command line and add it to current CONFIG

    config_path = get_config_path_from_args()
    CONFIG.add_config_path(config_path)

    setup_logging("inputs", "discovery")

    # ready to go

    logging.info("Program started")

    # determine spatial coverage
 
    c = WofsConfig(CONFIG)
    lat_range = c.get_lat_range()
    lon_range = c.get_lon_range()
    logging.info("Lat range is: %s", (lat_range, ))
    logging.info("Lon range is: %s", (lon_range, ))

    # determine time period

    time_interval = c.get_interval()
    logging.info(str(time_interval))

    # determine satellite list

    satellites = c.get_satellite_list()
    logging.info("Satellites: %s", (str(satellites), ))

    # get a CubeQueryContext (this is a wrapper around the API)

    cube = DatacubeQueryContext()

    # assemble datasets required by a WOfS run

    dataset_list = [DatasetType.ARG25, DatasetType.PQ25]

    # create a file containing ALL tiles to be processed

    inputs_dir = CONFIG.get('wofs','input_dir')
    mkdirs_if_not_present(inputs_dir)

    tiles_csv_path = "%s/tiles.csv" % (inputs_dir, )

    # now query the datacube to create the tiles.csv file

    tiles = cube.tile_list_to_file(lon_range, lat_range, satellites, time_interval, \
        dataset_list, tiles_csv_path)
    logging.info("%s created" % (tiles_csv_path, ))

    # sort that file by lat/lon
    # TODO: The following UNIX sort stage will not be necessary if 
    # when the API offers to sort the tile list by cell_id

    logging.info("sorting %s in cell ID order" % (tiles_csv_path, ))
    sorted_tiles_csv_path = "%s/sorted_tiles.csv" % (inputs_dir, )
    sort_csv_file(tiles_csv_path, sorted_tiles_csv_path, (7,8))
    logging.info("%s created" % (sorted_tiles_csv_path, ))

    # create a list of cells to include

    include_cells = None
    cell_list = CONFIG.get('coverage', 'include_cells', None)
    if cell_list is not None and len(cell_list) > 0:
        include_cells = json.loads(cell_list)

    # create a list of cells to exclude

    exclude_cells = None
    cell_list = CONFIG.get('coverage', 'exclude_cells', None)
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
        logging.info("%d tiles discovered" % (tile_count + all_tile_count, ))


    # create working directories if they do not exist

    mkdirs_if_not_present(CONFIG.get('wofs', 'pyramids_dir'))
    mkdirs_if_not_present(CONFIG.get('wofs', 'summaries_dir'))
    for cell_id in cell_ids:
        mkdirs_if_not_present(pjoin(CONFIG.get('wofs', 'bordered_elev_tile_path'), cell_id))
        mkdirs_if_not_present(pjoin(CONFIG.get('wofs', 'tsm_dir'), cell_id))
        mkdirs_if_not_present(pjoin(CONFIG.get('wofs', 'sia_dir'), cell_id))
        mkdirs_if_not_present(pjoin(CONFIG.get('wofs', 'extents_dir'), cell_id))

    
    logging.info("Program finished")

if __name__ == '__main__':
    main()
