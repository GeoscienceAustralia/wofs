#!/bin/env python
"""
Make water extents files for all tiles discovered in this WOfS run
"""

import os
import re
from os.path import join as pjoin, dirname, exists
import gc
import argparse
import logging

import rasterio as rio
from gaip import write_img  #todo: decouple from gaip

# import wofs
import wofs.utils.tools as tools  # import scatter, mkdirs_if_not_present
import wofs.utils.timeparser as timeparser  # import scatter, mkdirs_if_not_present
from wofs.utils.geobox import GriddedGeoBox
from wofs.utils.bordered_dsm import BorderedElevationTile
from wofs.utils.water_band import WaterBand
import wofs.utils.dsm

from wofs.waters.detree.classifier import WaterClassifier
import wofs.waters.detree.filters as filters

import luigi
from wofs.workflow.utils import FuzzyTileTarget, FuzzyShadowTileTarget, SingleUseLocalTarget, rm_single_use_inputs_after
from tempfile import mkdtemp
import csv
from datacube.api.model import DatasetType, Tile, Cell, Satellite
import datetime

from wofs.utils.sloper import Sloper
from wofs.waters.detree.extent_producer import WaterExtentProducer

CONFIG = luigi.configuration.get_config()
CSV_PATTERN = re.compile("cell_(?P<lon>-?\d+)_(?P<lat>-?\d+)_tiles.csv")
SCRATCH_DIR = tools.mkdirs_if_not_present( \
    CONFIG.get('wofs', 'scratch_dir', \
               os.getenv("PBS_JOBFS", mkdtemp(prefix="wofs_scratch_"))))
GTIFF = 'GTiff'


def get_cell_temp_dir(x, y):
    temp_base_dir = CONFIG.get('wofs', 'cell_temp_dir', None)
    if temp_base_dir is None:
        temp_base_dir = SCRATCH_DIR
    return pjoin(temp_base_dir, "cell_%03d_%04d" % (x, y))


def get_input_dir():
    return CONFIG.get('wofs', 'input_dir')


def get_output_dir():
    return CONFIG.get('wofs', 'summaries_dir')


def get_extent_tile_fuzzy_secs():
    return int(CONFIG.get('wofs', 'extent_tile_fuzzy_secs', '300'))


def get_shadow_tile_fuzzy_delta():
    return float(CONFIG.get('wofs', 'shadow_tile_fuzzy_delta', 0.25 / 24.0 / 365.25))


class NbarTile(luigi.ExternalTask):
    """
    Standardised surface reflectance data is input to the WOfS workflow
    """
    nbar_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.nbar_path)


class PqTile(luigi.ExternalTask):
    """
    Pixel quality mask is input to the WOfS workflow
    """
    pq_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.pq_path)


class DsmTile(luigi.ExternalTask):
    """
    Digital surface model is input to the WOfS workflow
    """

    dsm_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.dsm_path)


class BorderedElevation(luigi.Task):
    """
    An elevation tile (derived from DSM) with a 250 pixel border 
    around it - used for creation of shadow masks.
    """
    cell_x = luigi.IntParameter()
    cell_y = luigi.IntParameter()

    def requires(self):
        dsm_path = pjoin(CONFIG.get('wofs', 'dsm_path'), \
                         "DSM_%03d_%04d.tif" % (self.cell_x, self.cell_y))
        return DsmTile(dsm_path)

    def output(self):
        basename = "BET_%03d_%04d.tif" % (self.cell_x, self.cell_y)
        target = pjoin(CONFIG.get('wofs', 'bordered_elev_tile_path'), basename)
        return luigi.LocalTarget(target)

    def run(self):
        bet = BorderedElevationTile(self.cell_x, self.cell_y, CONFIG.get('wofs', 'dsm_path'))

        geobox, data = bet.get_data()
        write_img(data, filename=self.output().path, fmt=GTIFF, compress='lzw', geobox=geobox)


class TsmDir(luigi.ExternalTask):
    """
    Directory for Terrain Shadow Masks (TSMs) - basedir/shadow
    """
    x = luigi.IntParameter()
    y = luigi.IntParameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(
            CONFIG.get('wofs', 'tsm_dir'), \
            "%3d_%04d" % (self.x, self.y)))


class RayTracedShadowMask(luigi.Task):
    """
    Compute a shadow mask from the supplied DSM tile with 
    solar illumination properties relevant for the date/time of the 
    the associated NBAR tile. Output files stored in shadow dir
    """

    timestamp = luigi.Parameter()  # a ISO8601 string
    cell_x = luigi.IntParameter()
    cell_y = luigi.IntParameter()

    def requires(self):
        return \
            BorderedElevation(self.cell_x, self.cell_y), \
            TsmDir(self.cell_x, self.cell_y)

    def output(self):
        ts = tools.filename_from_datetime(self.timestamp)
        basename = "TSM_%3d_%04d_%s.tif" % (self.cell_x, self.cell_y, ts)
        target = os.path.join(self.input()[1].path, basename)
        return FuzzyShadowTileTarget(target, get_shadow_tile_fuzzy_delta())

    def run(self):
        with rio.open(self.input()[0].path) as bet_ds:
            bet_band = bet_ds.read(1)
            geobox = GriddedGeoBox.from_rio_dataset(bet_ds)
            utc = tools.datetime_from_iso8601(self.timestamp)
            (shadow_mask, geobox, metadata) = tools.compute_shadows(bet_band, geobox, utc)
            r = int((shadow_mask.shape[0] - 4000) / 2.0)
            c = int((shadow_mask.shape[1] - 4000) / 2.0)
            new_geobox = geobox.get_window_geobox(((r, r + 4000), (c, c + 4000)))
            write_img(shadow_mask[r:r + 4000, c:c + 4000], filename=self.output().path, \
                      fmt=GTIFF, geobox=new_geobox, tags=metadata, compress='lzw')


class SiaDir(luigi.ExternalTask):
    x = luigi.IntParameter()
    y = luigi.IntParameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(
            CONFIG.get('wofs', 'sia_dir'), \
            "%3d_%04d" % (self.x, self.y)))


class SolarIncidentAngleTile(luigi.Task):
    """
    Compute a solar incident angle tile from a DSM tile
    applicable for the date/time supplied
    """

    timestamp = luigi.Parameter()  # a ISO8601 string
    cell_x = luigi.IntParameter()
    cell_y = luigi.IntParameter()

    def requires(self):
        dsm_path = pjoin(CONFIG.get('wofs', 'dsm_path'), \
                         "DSM_%03d_%04d.tif" % (self.cell_x, self.cell_y))
        return \
            DsmTile(dsm_path), \
            SiaDir(self.cell_x, self.cell_y)

    def output(self):
        basename = "SIA_%3d_%04d_%s.tif" % (self.cell_x, self.cell_y, self.timestamp)
        target = os.path.join(self.input()[1].path, basename)
        return FuzzyShadowTileTarget(target, get_shadow_tile_fuzzy_delta())

    def run(self):
        # sloper = wofs.utils.sloper.Sloper(self.cell_x, self.cell_y, os.path.dirname(self.input()[0].path))
        sloperObj = Sloper(self.cell_x, self.cell_y, os.path.dirname(self.input()[0].path))

        utc = tools.datetime_from_iso8601(self.timestamp)
        geobox, data, metadata = sloperObj.get_solar_incident_deg(utc)
        write_img(data, filename=self.output().path, fmt=GTIFF, geobox=geobox, \
                  compress='lzw', tags=metadata)


class CellTempDir(luigi.Task):
    x = luigi.IntParameter()
    y = luigi.IntParameter()

    def complete(self):
        return os.path.exists(get_cell_temp_dir(self.x, self.y))

    def run(self):
        tools.mkdirs_if_not_present(get_cell_temp_dir(self.x, self.y))


class RawWaterExtent(luigi.Task):
    timestamp = luigi.Parameter()  # a ISO8601 string
    x = luigi.IntParameter()
    y = luigi.IntParameter()
    nbar_path = luigi.Parameter(significant=False)

    def requires(self):
        return \
            CellTempDir(self.x, self.y), \
            NbarTile(self.nbar_path)

    def output(self):
        return SingleUseLocalTarget(pjoin( \
            get_cell_temp_dir(self.x, self.y), \
            "rawWater_%s.tif" % (self.timestamp,)))

    def run(self):
        """ 
        Here we read the NBAR layers and
        apply the water detection classifier
        """
        water_extent, geobox = WaterClassifier.detect_water_in_nbar(self.nbar_path)
        write_img(water_extent, self.output().path, fmt=GTIFF, \
                  geobox=geobox, compress='lzw')

    # @rm_single_use_inputs_after


class NoDataFilterWaterExtent(luigi.Task):
    """
    Apply a no-data mask to a water extent and output the 
    masked data as a 'no_data_masked' extent
    """

    timestamp = luigi.Parameter()  # a ISO8601 string
    x = luigi.IntParameter()  # longitude of cell
    y = luigi.IntParameter()  # latitude of cell
    nbar_path = luigi.Parameter(significant=False)

    def requires(self):
        return \
            NbarTile(self.nbar_path), \
            RawWaterExtent(self.timestamp, self.x, self.y, self.nbar_path)

    def output(self):
        return SingleUseLocalTarget(pjoin( \
            get_cell_temp_dir(self.x, self.y), \
            "noDataMasked_%s.tif" % (self.timestamp,)))

    def run(self):
        with rio.open(self.nbar_path) as nbar_ds:
            nbar_bands = nbar_ds.read()
            nodata = nbar_ds.meta['nodata']
            geobox = GriddedGeoBox.from_rio_dataset(nbar_ds)
            with rio.open(self.input()[1].path) as water_ds:
                water_band = water_ds.read(1)
                water_band = filters.NoDataFilter().apply(water_band, nbar_bands, nodata)
        write_img(water_band, self.output().path, fmt=GTIFF, geobox=geobox, \
                  compress='lzw')


# @rm_single_use_inputs_after
class ContiguityFilterWaterExtent(luigi.Task):
    """
    Apply a contiguity mask to a water extent and output the 
    masked data as a 'contiguityMasked' extent
    """

    timestamp = luigi.Parameter()  # a ISO8601 string
    x = luigi.IntParameter()  # longitude of cell
    y = luigi.IntParameter()  # latitude of cell
    nbar_path = luigi.Parameter(significant=False)
    pq_path = luigi.Parameter(significant=False)

    def requires(self):
        return \
            PqTile(self.pq_path), \
            NoDataFilterWaterExtent(self.timestamp, self.x, self.y, self.nbar_path)

    def output(self):
        return SingleUseLocalTarget(pjoin( \
            get_cell_temp_dir(self.x, self.y), \
            "contiguityMasked_%s.tif" % (self.timestamp,)))

    def run(self):
        with rio.open(self.pq_path) as pq_ds:
            pq_band = pq_ds.read(1)
            geobox = GriddedGeoBox.from_rio_dataset(pq_ds)
            with rio.open(self.input()[1].path) as water_ds:
                water_band = water_ds.read(1)
                water_band = filters.ContiguityFilter(pq_band).apply(water_band)
        write_img(water_band, self.output().path, fmt=GTIFF, geobox=geobox, \
                  compress='lzw')


# @rm_single_use_inputs_after
class CloudFilterWaterExtent(luigi.Task):
    """
    Apply the cloud and cloud shadow masks to a water extent and output the 
    masked data as a 'cloudMasked' extent
    """

    timestamp = luigi.Parameter()  # a ISO8601 string
    x = luigi.IntParameter()  # longitude of cell
    y = luigi.IntParameter()  # latitude of cell
    nbar_path = luigi.Parameter(significant=False)
    pq_path = luigi.Parameter(significant=False)

    def requires(self):
        return \
            PqTile(self.pq_path), \
            ContiguityFilterWaterExtent(self.timestamp, self.x, self.y, self.nbar_path, self.pq_path)

    def output(self):
        return SingleUseLocalTarget(pjoin( \
            get_cell_temp_dir(self.x, self.y), \
            "cloudMasked_%s.tif" % (self.timestamp,)))

    def run(self):
        with rio.open(self.pq_path) as pq_ds:
            pq_band = pq_ds.read(1)
            geobox = GriddedGeoBox.from_rio_dataset(pq_ds)
            with rio.open(self.input()[1].path) as water_ds:
                water_band = water_ds.read(1)
                water_band = filters.CloudAndCloudShadowFilter(pq_band).apply(water_band)
        write_img(water_band, self.output().path, fmt=GTIFF, geobox=geobox, \
                  compress='lzw')


# @rm_single_use_inputs_after
class SolarIncidentFilterWaterExtent(luigi.Task):
    """
    Apply the solar incident angle  mask to a water extent and output the 
    masked data as a 'siaMasked' extent
    """

    timestamp = luigi.Parameter()  # a ISO8601 string
    x = luigi.IntParameter()  # longitude of cell
    y = luigi.IntParameter()  # latitude of cell
    nbar_path = luigi.Parameter(significant=False)
    pq_path = luigi.Parameter(significant=False)

    def requires(self):
        # print "SolarIncidentFilterWaterExtent requires()" 
        return [ \
            SolarIncidentAngleTile(self.timestamp, self.x, self.y), \
            CloudFilterWaterExtent(self.timestamp, self.x, self.y, self.nbar_path, self.pq_path)]

    def output(self):
        target = SingleUseLocalTarget(pjoin( \
            get_cell_temp_dir(self.x, self.y), \
            "siaMasked_%s.tif" % (self.timestamp,)))
        #  print "SolarIncidentFilterWaterExtent outputs %s exists: %s" % (target.path, str(os.path.exists(target.path)))
        return target

    def run(self):
        threshold = int(CONFIG.get('wofs', 'low_solar_incident_threshold', '30'))
        with rio.open(self.requires()[0].output().nearest_path()) as sia:
            sia_band = sia.read(1)
            geobox = GriddedGeoBox.from_rio_dataset(sia)
            with rio.open(self.input()[1].path) as water_ds:
                water_band = water_ds.read(1)
                water_band = filters.LowSolarIncidenceFilter(sia_band, threshold_deg=threshold).apply(water_band)
        write_img(water_band, self.output().path, fmt=GTIFF, geobox=geobox, \
                  compress='lzw')


# @rm_single_use_inputs_after
class TerrainShadowFilterWaterExtent(luigi.Task):
    """
    Apply the terrain shadow  mask to a water extent and output the 
    masked data as a 'tsmMasked' extent
    """

    timestamp = luigi.Parameter()  # a ISO8601 string
    x = luigi.IntParameter()  # longitude of cell
    y = luigi.IntParameter()  # latitude of cell
    nbar_path = luigi.Parameter(significant=False)
    pq_path = luigi.Parameter(significant=False)

    def requires(self):
        # print "SolarIncidentFilterWaterExtent requires()" 
        return [ \
            RayTracedShadowMask(self.timestamp, self.x, self.y), \
            SolarIncidentFilterWaterExtent(self.timestamp, self.x, self.y, self.nbar_path, self.pq_path)]

    def output(self):
        target = SingleUseLocalTarget(pjoin( \
            get_cell_temp_dir(self.x, self.y), \
            "tsmMasked_%s.tif" % (self.timestamp,)))
        return target

    def run(self):
        with rio.open(self.requires()[0].output().nearest_path()) as tsm:
            tsm_band = tsm.read(1)
            geobox = GriddedGeoBox.from_rio_dataset(tsm)
            with rio.open(self.input()[1].path) as water_ds:
                water_band = water_ds.read(1)
                water_band = filters.TerrainShadowFilter(tsm_band).apply(water_band)
        write_img(water_band, self.output().path, fmt=GTIFF, geobox=geobox, \
                  compress='lzw')


# @rm_single_use_inputs_after
class HighSlopeFilteredWaterExtent(luigi.Task):
    """
    Apply a high slope mask to a water extent and output the 
    masked data as a 'slopeMasked' extent
    """

    timestamp = luigi.Parameter()  # a ISO8601 string
    x = luigi.IntParameter()  # longitude of cell
    y = luigi.IntParameter()  # latitude of cell
    nbar_path = luigi.Parameter(significant=False)
    pq_path = luigi.Parameter(significant=False)

    def requires(self):
        dsm_path = pjoin(CONFIG.get('wofs', 'dsm_path'), \
                         "DSM_%03d_%04d.tif" % (self.x, self.y))
        return \
            DsmTile(dsm_path), \
            TerrainShadowFilterWaterExtent(self.timestamp, self.x, self.y, \
                                           self.nbar_path, self.pq_path)

    def output(self):
        return SingleUseLocalTarget(pjoin( \
            get_cell_temp_dir(self.x, self.y), \
            "slopeMasked_%s.tif" % (self.timestamp,)))

    def run(self):
        slope_limit_deg = float(CONFIG.get('wofs', 'slope_limit_degrees', '12.0'))
        with rio.open(self.input()[0].path) as dsm_ds:
            slope_data = dsm_ds.read(wofs.utils.dsm.SLOPE_BAND)
            geobox = GriddedGeoBox.from_rio_dataset(dsm_ds)
            with rio.open(self.input()[1].path) as water_ds:
                water_band = water_ds.read(1)
                filter = filters.HighSlopeFilter(slope_data,slope_limit_degrees=slope_limit_deg)
                water_band = filter.apply(water_band)
        write_img(water_band, self.output().path, fmt=GTIFF, geobox=geobox,compress='lzw')


# @rm_single_use_inputs_after
class LandSeaMaskedWaterExtent(luigi.Task):
    """
    Apply a land/sea mask to a water extent and output the 
    masked data as the final water extent
    """
    timestamp = luigi.Parameter()  # a ISO8601 string
    x = luigi.IntParameter()  # longitude of cell
    y = luigi.IntParameter()  # latitude of cell
    nbar_path = luigi.Parameter(significant=False)
    pq_path = luigi.Parameter(significant=False)

    def requires(self):
        return \
            PqTile(self.pq_path), \
            HighSlopeFilteredWaterExtent(self.timestamp, self.x, self.y, \
                                         self.nbar_path, self.pq_path)

    def output(self):
        return SingleUseLocalTarget(pjoin( \
            get_cell_temp_dir(self.x, self.y), \
            "landSeaMasked_%s.tif" % (self.timestamp,)))

    def run(self):
        with rio.open(self.pq_path) as pq_ds:
            pq_band = pq_ds.read(1)
            geobox = GriddedGeoBox.from_rio_dataset(pq_ds)
            with rio.open(self.input()[1].path) as water_ds:
                water_band = water_ds.read(1)
                water_band = filters.SeaWaterFilter(pq_band).apply(water_band)
                write_img(water_band, self.output().path, fmt=GTIFF, \
                          geobox=geobox, compress='lzw')


class ExtentsTopDir(luigi.ExternalTask):
    """  ensure the dir like  wofs/extents
    """

    def output(self):
        return luigi.LocalTarget(CONFIG.get('wofs', 'extents_dir'))


class ExtentsDir(luigi.Task):
    """  ensure existence of the likes of wofs/extents/149_-036
    """
    x = luigi.IntParameter()
    y = luigi.IntParameter()

    def requires(self):
        return ExtentsTopDir()

    def get_extents_dir(self):
        # return pjoin(CONFIG.get('wofs', 'extents_dir'), "%03d_%04d" % (self.x, self.y))
        return pjoin(self.input().path, "%03d_%04d" % (self.x, self.y))

    def output(self):
        extentcelldir = pjoin(self.input().path, "%03d_%04d" % (self.x, self.y))
        print ("output(): an extents/cell_id dir", extentcelldir)
        return luigi.LocalTarget(extentcelldir)

    def run(self):
        """
        Create the extents/cell_id dir for the cell
        """
        pathd = self.output().path
        print "run(): an extents/cell_id dir", pathd
        tools.mkdirs_if_not_present(pathd)


# @rm_single_use_inputs_after
class WaterExtent(luigi.Task):  # keep this original
    """
    Collect metadata and write the final Water Extent (WOFL) file
    """
    timestamp = luigi.Parameter()  # a ISO8601 string
    x = luigi.IntParameter()
    y = luigi.IntParameter()
    nbar_path = luigi.Parameter(significant=False)
    pq_path = luigi.Parameter(significant=False)

    def requires(self):
        return \
            ExtentsDir(self.x, self.y), \
            LandSeaMaskedWaterExtent(self.timestamp, self.x, self.y, \
                                     self.nbar_path, self.pq_path)

    def output(self):
        basename = os.path.basename(self.nbar_path).replace("NBAR", "WATER")
        basename = basename.replace("vrt", "tif")

        tools.mkdirs_if_not_present(self.input()[0].path)  # make a dir like wofs/extents/149_-036

        return FuzzyTileTarget(pjoin(self.input()[0].path, basename),
                               get_extent_tile_fuzzy_secs())

    def run(self):
        """
        Read the filtered water band, create statistics
        and write the output
        """
        with rio.open(self.input()[1].path) as water_ds:
            water_band = water_ds.read(1)
            metadata = WaterBand(water_band).getStatistics()
            geobox = GriddedGeoBox.from_rio_dataset(water_ds)
            #
            write_img(water_band, filename=self.output().path, fmt=GTIFF, \
                      geobox=geobox, compress='lzw', tags=metadata)


###########################################################################################
# @rm_single_use_inputs_after
class WaterExtent2(luigi.Task):
    """ A major Luigi task to derive water extent step-by-step through an algorithm,
     and write the final Water Extent (WOFL) file
    """
    timestamp = luigi.Parameter()  # a ISO8601 string
    x = luigi.IntParameter()
    y = luigi.IntParameter()
    nbar_path = luigi.Parameter(significant=False)
    pq_path = luigi.Parameter(significant=False)

    # require what tasks to be completed?
    def requires(self):
        return ExtentsDir(self.x, self.y)
        # ,LandSeaMaskedWaterExtent(self.timestamp, self.x, self.y,self.nbar_path, self.pq_path)

    # return what outcome/result?
    def output(self):
        basename = os.path.basename(self.nbar_path).replace("NBAR", "WATER")
        basename = basename.replace("vrt", "tif")

        tools.mkdirs_if_not_present(self.input().path)  # make a dir like wofs/extents/149_-036

        return FuzzyTileTarget(pjoin(self.input().path, basename), get_extent_tile_fuzzy_secs())

    # The process to produce the output
    def run(self):
        """
        Derive the filtered water band using the WaterExtentProducer
        compute statistics and write output file
        """
        # with rio.open(self.input()[1].path) as water_ds:
        #     water_band = water_ds.read(1)
        #     metadata = wofs.WaterBand(water_band).getStatistics()
        #     geobox = GriddedGeoBox.from_rio_dataset(water_ds)
        #
        #     write_img(water_band, filename=self.output().path, fmt=GTIFF, \
        #         geobox=geobox, compress='lzw', tags=metadata)

        logging.getLogger().debug(" run():  WaterExtent2 task ********** ")

        wpro = WaterExtentProducer(self.nbar_path, self.pq_path)

        wpro.waterband2file(self.output().path)

        # water_band.tofile(self.output().path )

        return self.output().path

    ############## Separate the process logic for WaterSummary ########################################


class WaterExtentsMain(luigi.Task):
    """
    Run all Luigi tasks to generate water extents for a single datacube cell
    then count the number of water_extents, output to a temp dumpy file.

    Switch LuigiTasks:
    WaterExtent:    The original algorithm with all intermediate water_band saved to scratch/, for debugging
    WaterExtent2:   Combined Water Algorithm and Filters to produce water extent.
    """

    cell_csv = luigi.Parameter()

    def get_xy(self):
        m = CSV_PATTERN.match(self.cell_csv)
        return (int(m.group(1)), int(m.group(2)))

    def requires(self):
        """
        Each run requires the collection of water extents for the Cell, for given list of nbar pq data
        """
        tasks = []
        with open(pjoin(get_input_dir(), self.cell_csv), "rb") as f:
            reader = csv.DictReader(f)
            for record in reader:
                tile = Tile.from_csv_record(record)
                # TODO: Remove hack where timestamp is extracted from NBAR file name
                # once AGDC pull request #65 is approved
                # Code should be:    tile.start_datetime.isoformat(), \
                tasks.append(WaterExtent2( \
                    timeparser.find_datetime(tile.datasets[DatasetType.ARG25].path).isoformat(), \
                    self.get_xy()[0], \
                    self.get_xy()[1], \
                    tile.datasets[DatasetType.ARG25].path, \
                    tile.datasets[DatasetType.PQ25].path))

        print "Water Extents Main requires %d tiles" % (len(tasks))
        return tasks

    def output(self):
        """ 
        Outputs a summary/dummy_water_sum for each cell
        """
        dtstamp = datetime.datetime.utcnow().isoformat()  # 2016-04-22T05:05:29.929482
        dtstamp = dtstamp.replace(":", "")
        target = luigi.LocalTarget(pjoin( \
            get_output_dir(), \
            # "waterSummary_%03d_%04d.tiff" %  self.get_xy()))
            "dummy_water_sum_%03d_%04d_AT_" % self.get_xy() + dtstamp))

        return target

    def run(self):
        """
        After all water extents tasks are completed, count the number of tile/files
        write the number into an output dummy_water-sum_tileId_AT_datetimestamp 
        """
        path = self.output().path
        waterext_cnt = 0
        for task in self.requires():
            waterext_cnt += 1

        # debug log
        logging.getLogger().info("Number of water_extent tiles: %s" % str(waterext_cnt))

        with open(path, 'w') as outf:
            outf.write(str(waterext_cnt) + '\n')

        return path


#####################################################################
#  tasks distributer

def main(nnodes=1, nodenum=1):
    # create output directory

    tools.mkdirs_if_not_present(get_output_dir())

    # gather the Cells to process

    cells = sorted([f for f in os.listdir(get_input_dir()) \
                    if CSV_PATTERN.match(f)])
    #    if CSV_PATTERN.match(f) and "150_-034" in f])
    our_cells = [cell_id for cell_id in tools.scatter(cells, nnodes, nodenum)]
    tasks = [WaterExtentsMain(c) for c in our_cells]

    # launch luigi with multiple workers (one per CPU)

    workers = int(os.getenv('PBS_NCPUS', '1')) / nnodes
    print "SCRATCH_DIR=", SCRATCH_DIR
    print "Host=%s, PID=%d, nnodes=%d, nodenum=%d, tasks_count=%d, workers=%d, our_cells=%s" % \
          (os.uname()[1], os.getpid(), nnodes, nodenum, len(tasks), workers, str(our_cells))

    if workers > 20:
        tools.die("Too many workers specified, check job setup!!")

    luigi.build(tasks, local_scheduler=True, workers=workers)


##################################################################################
if __name__ == '__main__':
    size = int(os.getenv('PBS_NNODES', '1'))
    rank = int(os.getenv('PBS_VNODENUM', '1'))
    main(size, rank)
