#################################################
# Usage:
#   export PYTHONPATH=.:/g/data/u46/fxz547/Githubz/agdc-v2
#   python agdc_dao.py
#################################################

import os, sys
from collections import defaultdict

import xarray as xr
import xarray.ufuncs

import datacube
from datacube.api import GridWorkflow, make_mask  # masking

from datacube.index import index_connect
from datacube.config import LocalConfig
# from datacube.api._conversion import to_datetime
from pandas import to_datetime

# from wofs.waters.detree.classifier import WaterClassifier
# import wofs.waters.detree.filters as filters
import numpy
from scipy import stats

import logging

# logging.basicConfig(level=logging.INFO)
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)


class AgdcDao():
    """
    Datacube Data Access Object. Default prod
    """

    def __init__(self, dc=None):

        if dc is None:
            dc = datacube.Datacube(app='wofs-dev')  # default use $HOME/.datacube.conf
            # to use a specific configfile: dc = datacube.Datacube(config=/path2/your.datacube.conf', app='wofs-dev')

        self.gw = GridWorkflow(dc, product='ls5_nbar_albers')  # product is used to derive grid_spec

        return

    def get_cells_list(self, qdict):
        """
        Get a List of all relevant cell indexes by a dictionary qdict
        cells = dc.list_cells(product='nbar', longitude=lon_range, latitude=lat_range, time=time_interval, platform=satellites)

        all NBAR cells dc.list_cells(product='nbar')

        dc.list_cells(product='nbar', longitude=149, latitude=-35, time=('1990', '2015'))
        :return:
        """
        # Query parameters:

        logging.debug(qdict)

        # {"product": "nbar", "longitude": (149, 150), "latitude": (-40, -41), "time": ('1900-01-01', '2016-03-20')}
        # product_type= nbar | pqa
        # cells = gw.list_cells(product_type='pqa',
        # longitude=(149.06,149.18), latitude=(-35.27, -35.33),time=('1996-01-01', '2016-03-20'))

        cells = self.gw.list_cells(**qdict)
        # ** unpack a dict into a key-value pair argument matching the function's signature

        logging.info(str(cells))
        # return cells_filtered_list =filter_cell_list(cells)
        return cells

    # ----------------------------------------------------------------
    def get_tiles_for_wofs(self, qdict, inputs_dir):
        """
        query to discover all relevant tilesets (nbar + pqa) for a list of cells, write them into inputs_dir/cell_id
        :return: path2_all_tiles
        """

        all_tiles_file = 'all_tiles.txt'
        path2_all_tiles = os.path.join(inputs_dir, all_tiles_file)

        tile_store = self.get_nbarpqa_tiles(qdict)

        tile_keys = sorted(tile_store.keys())

        with  open(path2_all_tiles, 'w') as infile:
            for eachtile in tile_keys:
                infile.write(str(eachtile) + "\n")

        #
        # for time in sorted(stack):
        #     tileset = stack[time]  # This should be a pair of nbar-pqa
        #     if 'nbar' in tileset and 'pqa' in tileset:
        #         logging.debug("This cell has both nbar and pq data at time=%s" % str(time))
        #         logging.debug("%s, %s, %s", cell, to_datetime(time), len(tileset))  # , type(tiles)
        #
        #         nbar_pqa_pair = []
        #         for product, tile in tileset.items():
        #             nbar_pqa_pair.append((product, tile))
        #
        #         cell_tiles.append(nbar_pqa_pair)
        #     else:
        #         logging.warn("Cell = %s : nbar-pqa mismatching tiles at time=%s .. Skipping", str(cell), time)
        #
        # cell_id = "abc%s_%s.txt" % (cell)  # a txt/csv filename based on albers cellindex
        # fname = os.path.join(inputs_dir, cell_id)
        # with  open(fname, 'w') as infile:
        #     for eachtile in cell_tiles:
        #         infile.write(str(eachtile) + "\n")

        return path2_all_tiles

    # ----------------------------------------------------------------------------------
    def get_nbarpqa_tiles(self, qdict):

        # Nbar tiles
        nbar_tiles = self.gw.list_tiles(product='ls5_nbar_albers', **qdict)
        # ,longitude=(149.06, 149.18), latitude=(-35.27, -35.33),time=('1996-01-01', '2016-03-20'))

        # Pixel Quality Tiles
        pq_tiles = self.gw.list_tiles(product='ls5_pq_albers', **qdict)

        # Cell, Time -> Product -> TileDef
        tile_def = defaultdict(dict)

        for cell, tiles in nbar_tiles.items():
            for time, tile in tiles.items():
                tile_def[cell, time]['nbar'] = tile

        for cell, tiles in pq_tiles.items():
            for time, tile in tiles.items():
                tile_def[cell, time]['pqa'] = tile

        for celltime, products in tile_def.items():
            if len(products) < 2:
                logging.warn('Only found {products} at cell={cell} and time={time}'.format(
                    products=products.keys(), cell=cell, time=time))
            else:
                logging.debug('%s,%s', celltime, len(products))

        return tile_def

    ##----------------------------------------------------------------
    def get_nbarpqa_tiles_by_cell(self, acell, qdict):
        """
        return a list of tiles
        :param acell: a cell index tuple (15, -40)
        :return:
        """
        # gw.list_tiles((15,-40), product='ls5_nbar_albers')

        nbar_tiles = self.gw.list_tiles(acell, product='ls5_nbar_albers',
                                        **qdict)  # , platform='LANDSAT_8')  # ,time=('2000', '2007'))
        pq_tiles = self.gw.list_tiles(acell, product='ls5_pq_albers',
                                      **qdict)  # , platform='LANDSAT_8')  # , time=('2000', '2007'))

        if (len(pq_tiles) == len(nbar_tiles)):
            logging.debug("The cells have %s nbar and %s pq tiles", len(nbar_tiles), len(pq_tiles))
        else:
            logging.warn("NBAR-PQA tiles mismatch: The cells have %s nbar and %s pq tiles", len(nbar_tiles),
                         len(pq_tiles))

        # Cell, Time -> Product -> TileDef
        tile_def = defaultdict(dict)

        for cell, tiles in nbar_tiles.items():
            for time, tile in tiles.items():
                tile_def[cell, time]['nbar'] = tile

        for cell, tiles in pq_tiles.items():
            for time, tile in tiles.items():
                tile_def[cell, time]['pqa'] = tile

        for celltime, products in tile_def.items():
            if len(products) < 2:
                logging.warn('Only found {products} at cell: {cell} at time: {time}'.format(
                    products=products.keys(), cell=cell, time=time))
            else:
                logging.debug('%s,%s', celltime, len(products))

        return tile_def

    ######################################################
    def get_one_nbarpq_tiledata(self, acellindex, timestamp, qdict=None):
        """
        for a given cell-index and a timestamp, query qdict, return dataarrays of nbar and pqa tile-pair
        matching exactly the cellindex and timestamp
        :param timestamp: =numpy.datetime64('1990-03-03T10:11:16.000000000+1100'))
        :param cellindex: = (15, -40)
        :return: list of tiles-pairs [(nbar,pq), ] with 1 item
        """

        logging.debug("The input cell-index is %s", acellindex)

        # sys.exit(10)

        tiledatas = []

        tile_store = self.get_nbarpqa_tiles_by_cell(acellindex, qdict)

        #tile_keys = tile_store.keys()
        tile_keys = sorted(tile_store.keys())

        # for key in tile_keys[:5]:  # only first few tiles
        for key, tile in tile_store.items():  # trying to load all tiles may exceed resource limit, get killed in raijin
            # print key, tile_store[key]
            cellid = key[0]
            ts = key[1]
            logging.debug("the cellid and timestamp %s %s of the tile", cellid, ts)

            if (cellid == acellindex and ts == timestamp):  # only load the required data

                nbar_tile = tile_store[key]['nbar']

                pqa_tile = tile_store[key]['pqa']

                nbar_data = self.gw.load(acellindex, nbar_tile)  # is cell really necessary??
                pqa_data = self.gw.load(acellindex, pqa_tile)

                tiledatas.append((key, nbar_data, pqa_data))

            else:
                logging.debug("skipping %s", str(key))
                pass


                # for time in sorted(stack):
                # print ("time=", time)

                # tileset = stack[time]

                # print "Cell {} at time [{:%Y-%m-%d}] has {} tiles: ".format(cellindex, to_datetime(time), len(tileset))
                # for product, tile in tileset.items():
                #     print product, tile

                # if 'nbar' in tileset and 'pqa' in tileset:
                #
                #     logging.info("This cell has both nbar and pq data at time=%s" % str(time))
                #
                #     nbar_tile_query, platform, path2file = tileset['nbar']
                #     # This will get replaced by the semantic layer
                #     if platform in ('LANDSAT_5', 'LANDSAT_7'):
                #         variables = ['band_1', 'band_2', 'band_3', 'band_4', 'band_5', 'band_7']
                #     elif platform in ('LANDSAT_8'):
                #         variables = ['band_2', 'band_3', 'band_4', 'band_5', 'band_6', 'band_7']
                #
                #     # nbar_tile = self.dao.get_data_array_by_cell(variables=variables, set_nan=True, **nbar_tile_query)
                #     nbar_tile = self.dao.get_data_array_by_cell(variables=variables, set_nan=False, **nbar_tile_query)
                #
                #     pq_tile_query,  platform, path2file = tileset['pqa']
                #     pq_tile_ds = self.dao.get_dataset_by_cell(**pq_tile_query)
                #     pq_tile = pq_tile_ds['pixelquality']
                #
                #     # print "{:%c}\tnbar shape: {}\tpq shape: {}".format(to_datetime(time), nbar_tile.shape, pq_tile.shape)
                #     # Wed Dec 27 23:45:28 2006	nbar shape: (6, 4000, 4000)	pq shape: (4000, 4000)
                #
                #     tiledatas.append((time, platform, nbar_tile, pq_tile))
                #
                #     # break  # use break if just do the first one as a test...
                # else:
                #     logging.warn("WARN missing data at time=%s", time)

        return tiledatas

######################################################
    def get_multi_nbarpq_tiledata(self, acellindex, qdict=None, maxtiles=10):
        """
        for a given cell-index, and query qdict, return dataarrays of nbar and pqa tile-pair
        :param maxtiles: The maxi number of tiles can return
        :param cellindex: = (15, -40)
        :return: list of tiles-pairs [(nbar,pq), ] not more than maxtiles
        """

        logging.debug("The input cell-index is %s", acellindex)

        # sys.exit(10)

        tiledatas = []

        tile_store = self.get_nbarpqa_tiles_by_cell(acellindex, qdict)

        tile_keys = sorted(tile_store.keys())

        logging.info("Number of tiles found is %s. But only the first %s tiles will be returned", len(tile_keys), maxtiles)

        for key in tile_keys[:maxtiles]:  # will load and returnthe first few tiles only, to avoid resource-out
        #for key, tile in tile_store.items():  # trying to load all tiles may exceed resource limit, get killed in raijin
            # print key, tile_store[key]
            cellid = key[0]
            ts = key[1]
            logging.debug("the cellid and timestamp %s %s of the tile", cellid, ts)

            nbar_tile = tile_store[key]['nbar']

            pqa_tile = tile_store[key]['pqa']

            nbar_data = self.gw.load(acellindex, nbar_tile)  # is cell really necessary??
            pqa_data = self.gw.load(acellindex, pqa_tile)

            tiledatas.append((key, nbar_data, pqa_data))

        return tiledatas
################################################################################################
# ----------------------------------------------------------
if __name__ == "__main__":
    import matplotlib.pyplot as plt

    # OK: qdict={'latitude': (-36.0, -35.0), 'platform': ['LANDSAT_5', 'LANDSAT_7', 'LANDSAT_8'], 'longitude': (149.01, 150.1), 'time': ('1990-01-01', '2016-03-31')}

    # OK qdict={'latitude': (-36.0, -35.0), 'platform': ['LANDSAT_5'], 'longitude': (149.01, 155.1), 'time': ('1990-01-01', '1990-03-31')}
    # OK qdict={'platform': ['LANDSAT_5'],  'time': ('1990-01-01', '1990-03-31')}

    cellindex = (15, -40)
    # cellindex = (15, -41)

    qdict = {'platform': ['LANDSAT_5'], 'time': ('1990-01-01', '1991-03-31')}

    # grep '15, -40' /g/data/u46/fxz547/wofs2/fxz547_2016-06-14T08-41-08/inputs/all_tiles.txt
    # tilekey=((15, -40), numpy.datetime64('1990-03-03T10:11:16.000000000+1100'))
    # ((15, -40), numpy.datetime64('1990-03-03T10:11:39.000000000+1100'))
    # ((15, -40), numpy.datetime64('1990-04-04T09:10:42.000000000+1000'))

    ts = numpy.datetime64('1990-03-03T10:11:16.000000000+1100')

    dcdao = AgdcDao()

    dcdao.get_tiles_for_wofs(qdict, '/g/data1/u46/users/fxz547/wofs2/')

    #tile_data = dcdao.get_one_nbarpq_tiledata(cellindex, ts, qdict)
    tile_data = dcdao.get_multi_nbarpq_tiledata(cellindex, qdict)

    print ("Number of (nbar-pqa) tile data found = ", len(tile_data))

    icounter=0
    for (celltime_key, nbar, pq) in tile_data[:10]:  # do the first few tiles of the list
        icounter=icounter+1

        print (celltime_key, nbar, pq)

        print (type(nbar), type(pq))

        plt.figure(icounter)
        plt.imshow(nbar.green.isel(time=0))
        plt.colorbar(orientation='vertical', shrink=0.8, label='nbar.green')
        plt.show()
