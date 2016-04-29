
"""
This is an implementation of a decision-tree machine-learning (data-mining classification) algorithm
to derive water extent from surface reflectance data (NBAR) and associated ancillary PQ, and DSM tiles.

The result water extent image is 1-band uint8 image tile
Each of the pixels of the image has one of the code-types:

    0=Dry,1=NoData, 2=no_contiguity, 4=over_sea, 8=terrain_shadow,
    16= high_slope, 32=cloud_shadow, 64=cloud64, 128=water

Author:     fei.zhang@ga.gov.au
Date:       2016-04-22

"""

from wofs.waters.detree.classifier import WaterClassifier
import wofs.waters.detree.filters as filters
from gaip import write_img
import rasterio as rio

GTIFF='GTiff'

###########################################################################################
class WaterExtentProducer(object):

    """
    From NBAR, PQ, and DSM tiles, derive a 1-band uint8 image (tile), called water extent
    the pixels of the water extent image are classified into one of the code-types:
    0=Dry,1=NoData, 2=no_contiguity, 4=over_sea, 8=terrain_shadow,
    16= high_slope, 32=cloud_shadow, 64=cloud64, 128=water
    """


    def __init__(self, nbarimg, pqimg, dsmpath=None):
        """
        :param nbarimg: 6xrowxcol numpy.array, representing the 6-bands NBAR (or NBARt) image
        :param pqimg:   1-band  numpy.array, associated to the NBAR image
        :param dsmimg:  dsm model tile or image, covering the spatial extent of the tile under consideration
        :return:
        """

        self.nbar_path=nbarimg
        self.pq_path= pqimg
        self.dsm_path=dsmpath

        return


    def derive_water_extent(self):
        """
        call a water classification algorithm:
        (band arithmetics and apply filters; OR  RandomForest, GreyMagic, etc)
         to generate a water extent image with the same size as the input nbar/pg tile
        :return: water extent image - 1band-ubyte
        """
        #1. raw water extent
        water_band, geobox = WaterClassifier.detect_water_in_nbar(self.nbar_path) # wofs.classifier.detect_water_in_nbar

        #2 Nodata filter
        with rio.open(self.nbar_path) as nbar_ds:
            nbar_bands = nbar_ds.read()
            nodata = nbar_ds.meta['nodata']

        water_band = filters.NoDataFilter().apply(water_band, nbar_bands, nodata)

        #write_img(water_band, self.output().path, fmt=GTIFF, geobox=geobox, compress='lzw')

        #3
        with rio.open(self.pq_path) as pq_ds:   pq_band = pq_ds.read(1)

        water_band =filters.ContiguityFilter(pq_band).apply(water_band)
        #write_img(water_band, self.output().path, fmt=GTIFF, geobox=geobox, compress='lzw')

        #4
        #with rio.open(self.pq_path) as pq_ds: pq_band = pq_ds.read(1)
            #geobox = GriddedGeoBox.from_rio_dataset(pq_ds)

        water_band = filters.CloudAndCloudShadowFilter(pq_band).apply(water_band) #compare with scratch/cellid/files


        # TODO: Combined SolarIncidentAngle, TerrainShadow, HighSlope Masks. They all use database DSM tiles.
        # Computationally expensive and re-projection required.
        # 5 SIA #6 TerrainShadow #7 HighSlope

        #TODO: water_band=SolarTerrainShadowSlope(self.dsm_path).filter(water_band)

        #8 Land-Sea. This is the last Filter mask out the Sea pixels as flagged in PQ band
        # using the pq_band read in step- 3 and 4

        water_band = filters.SeaWaterFilter(pq_band).apply(water_band)
        #write_img(water_band, self.output().path, fmt=GTIFF, geobox=geobox, compress='lzw')


        return (water_band, geobox)  # filtered water_band and geobox

    def waterband2file(self, outfile):
        """
        End-user entry point to use this class. Customised to output an gtiff/nc file or visualize the image,
        :return:
        """
        water_band, geob = self.derive_water_extent()

        #do things with the water extent, write out to file.
        #numpy array to raw binary file water_band.tofile(outfile)
        write_img(water_band, outfile, fmt='GTiff', geobox=geob, compress='lzw')  # write fun from ga-neo-landsat-processor/gaip/data.py

        return water_band

###############################################################################################
