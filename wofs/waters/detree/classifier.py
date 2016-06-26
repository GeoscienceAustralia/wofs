#!/usr/bin/env python
######################################################
# This python module implements Norman Mueller's Decision Tree algorithm
# to classify NBAR imagery pixels into Water, NotWater, NoData.
# An earlier version of the code can be found in ga-neo-nfrip/system/water/WaterClassifier.py
#
# @copyright:   Geoscience Australia
#
# @Developer:   Fei.Zhang@ga.gov.au
#
# @Date:        2016-04-15
#####################################################

import numpy
import logging
import gc
import argparse
from osgeo import gdal

import os
import rasterio
# from wofs.utils.geobox import GriddedGeoBox

# from datacube.api.model import BANDS
# from datacube.api.model import DatasetType, Satellite

logger = logging.getLogger(__name__)


class WaterClassifier(object):
    """
    WaterClassifier instance classify NBAR images by locating the presence of surface water
    """

    def __init__(self):
        self._version = 7

    def getVersion(self):
        return self._version

    @staticmethod
    def gdal_to_numpy_dtype(val):
        return {
            1: 'uint8',
            2: 'uint16',
            3: 'int16',
            4: 'uint32',
            5: 'int32',
            6: 'float32',
            7: 'float64',
            8: 'complex64',
            9: 'complex64',
            10: 'complex64',
            11: 'complex128',
        }.get(val, 'float64')

    def detect_water_in_nbar(self, nbar_path):
        """
        Apply the water detection function to the supplied NBAR file and
        return a raw water extents array
        """

        satellite = Satellite[os.path.basename(nbar_path).split('_')[0]]
        bands = BANDS[DatasetType.ARG25, satellite]
        bands = [
            bands.BLUE.value,
            bands.GREEN.value,
            bands.RED.value,
            bands.NEAR_INFRARED.value,
            bands.SHORT_WAVE_INFRARED_1.value,
            bands.SHORT_WAVE_INFRARED_2.value]

        logger.debug("detect_water_in_nbar %s using bands %s" % (nbar_path, str(bands)))

        with rasterio.open(nbar_path, "r") as ds:
            geobox = GriddedGeoBox.from_dataset(ds)
            nbar_data = ds.read(indexes=bands)
            logger.debug("nbar data read, calling classifer")

            water_band = self.classify(nbar_data)
            logger.debug("classification done")

        return water_band, geobox

    def classify(self, images, float64=False):
        """
        Produce a water classification image from the supplied images (6 bands of an NBAR, multiband Landsat image)
        This method evaluates N.Mueller's decision tree as follows:


                        -----------------------------N1---------------------------------
                        |                                                              |
                        |                                                              |
                     ---N2-----                                           -------------N21---------------------
                     |        |                                           |                                   |
                     |        |                                           |                                   |
           ----------N4----   N3                                    ------N22---                           ---N35-------
           |              |                                         |          |                           |           |
           |              |                                         |          |                           |           |
        ---N5---       ---N8--------------                       ---N24----    N23                      ---N37------   N36
        |      |       |                 |                       |        |                             |          |
        |      |       |                 |                       |        |                             |          |
        N6     N7   ---N12------------   N9             ---------N26---   N25                        ---N39-----   N38
                    |                |                  |             |                              |         |
                    |                |                  |             |                              |         |
                 ---N16---        ---N13---             N27   --------N28---                   ------N41---    N40
                 |       |        |       |                   |            |                   |          |
                 |       |        |       |                   |            |                   |          |
                 N17  ---N18---   N14     N15              ---N29---    ---N30---           ---N43---     N42
                      |       |                            |       |    |       |           |       |
                      |       |                            |       |    |       |           |       |
                      N19     N20                          N31     N32  N33     N34         N44     N45


    :param images:
        A 3D numpy array ordered in (bands,rows,columns), containing the spectral data.
        It is assumed that the spectral bands follow Landsat 5 & 7, Band 1, Band 2, Band 3, Band 4, Band 5, Band 7.

    :param float64:
        Boolean keyword. If set to True then the data will be converted to type float64 if not already float64.
        Default is False.

    :return:
        A 2D numpy array of type UInt8.  Values will be 0 for No Water, 1 for Unclassified and 128 for water.

    :notes:
        The input array will be converted to type float32 if not already float32.
        If images is of type float64, then images datatype will be left as is.

    :transcription:
        Transcribed from a Tree diagram output by CART www.salford-systems.com
        Josh Sixsmith; joshua.sixsmith@ga.gov.au

        """

        logger = logging.getLogger("WaterClasserfier")
        logger.debug("Started")

        def band_ratio(a, b):
            """
            Calculates a normalised ratio index.
            """
            c = (a - b) / (a + b)
            return c

        dims = images.shape
        if len(dims) == 3:
            bands = dims[0]
            rows = dims[1]
            cols = dims[2]
        else:
            rows = dims[0]
            cols = dims[1]

        dtype = images.dtype

        # Check whether to enforce float64 calcs, unless the datatype is already float64
        # Otherwise force float32
        if float64:
            if (dtype != 'float64'):
                images = images.astype('float64')
        else:
            if (dtype == 'float64'):
                # Do nothing, leave as float64
                images = images
            elif (dtype != 'float32'):
                images = images.astype('float32')

        classified = numpy.ones((rows, cols), dtype='uint8')

        NDI_52 = band_ratio(images[4], images[1])
        NDI_43 = band_ratio(images[3], images[2])
        NDI_72 = band_ratio(images[5], images[1])

        b1 = images[0]
        b2 = images[1]
        b3 = images[2]
        b4 = images[3]
        b5 = images[4]
        b7 = images[5]

        # Lets start going down the trees left branch, finishing nodes as needed
        # Lots of result arrays eg r1, r2 etc of type bool are created
        # These could be recycled to save memory, but at the moment they serve to show the tree structure
        # Temporary arrays of type bool (_tmp, _tmp2) are used to combine the boolean decisions
        r1 = NDI_52 <= -0.01

        r2 = b1 <= 2083.5
        classified[r1 & ~r2] = 0  # Node 3

        r3 = b7 <= 323.5
        _tmp = r1 & r2
        _tmp2 = _tmp & r3
        _tmp &= ~r3

        r4 = NDI_43 <= 0.61
        classified[_tmp2 & r4] = 128  # Node 6
        classified[_tmp2 & ~r4] = 0  # Node 7

        r5 = b1 <= 1400.5
        _tmp2 = _tmp & ~r5
        r6 = NDI_43 <= -0.01
        classified[_tmp2 & r6] = 128  # Node 10
        classified[_tmp2 & ~r6] = 0  # Node 11

        _tmp &= r5

        r7 = NDI_72 <= -0.23
        _tmp2 = _tmp & ~r7
        r8 = b1 <= 379
        classified[_tmp2 & r8] = 128  # Node 14
        classified[_tmp2 & ~r8] = 0  # Node 15

        _tmp &= r7

        r9 = NDI_43 <= 0.22
        classified[_tmp & r9] = 128  # Node 17

        _tmp &= ~r9

        r10 = b1 <= 473
        classified[_tmp & r10] = 128  # Node 19
        classified[_tmp & ~r10] = 0  # Node 20

        # Left branch is completed; cleanup
        logger.debug("B4 cleanup 1")
        del r2, r3, r4, r5, r6, r7, r8, r9, r10
        gc.collect()
        logger.debug("cleanup 1 done")

        # Right branch of the tree
        r1 = ~r1

        r11 = NDI_52 <= 0.23
        _tmp = r1 & r11

        r12 = b1 <= 334.5
        _tmp2 = _tmp & ~r12
        classified[_tmp2] = 0  # Node 23

        _tmp &= r12

        r13 = NDI_43 <= 0.54
        _tmp2 = _tmp & ~r13
        classified[_tmp2] = 0  # Node 25

        _tmp &= r13

        r14 = NDI_52 <= 0.12
        _tmp2 = _tmp & r14
        classified[_tmp2] = 128  # Node 27

        _tmp &= ~r14

        r15 = b3 <= 364.5
        _tmp2 = _tmp & r15

        r16 = b1 <= 129.5
        classified[_tmp2 & r16] = 128  # Node 31
        classified[_tmp2 & ~r16] = 0  # Node 32

        _tmp &= ~r15

        r17 = b1 <= 300.5
        _tmp2 = _tmp & ~r17
        _tmp &= r17
        classified[_tmp] = 128  # Node 33
        classified[_tmp2] = 0  # Node 34

        _tmp = r1 & ~r11

        r18 = NDI_52 <= 0.34
        classified[_tmp & ~r18] = 0  # Node 36
        _tmp &= r18

        r19 = b1 <= 249.5
        classified[_tmp & ~r19] = 0  # Node 38
        _tmp &= r19

        r20 = NDI_43 <= 0.45
        classified[_tmp & ~r20] = 0  # Node 40
        _tmp &= r20

        r21 = b3 <= 364.5
        classified[_tmp & ~r21] = 0  # Node 42
        _tmp &= r21

        r22 = b1 <= 129.5
        classified[_tmp & r22] = 128  # Node 44
        classified[_tmp & ~r22] = 0  # Node 45

        logger.debug("completed")

        return classified

    ##########################################################################
    def classify_by_pickled_model(self, images, float64=False):
        """
        Use scikit-learn and sample data to train a classification model (and pickle it if do not want to re-train).
        This module is to load the pickled model and apply it to predict water/nowater pixels.
        Todo: Refactoring to removed Hard-coded things.

        :param images: standard nbar images(6, nrow, ncol)
        :param float64: ignore
        :return: a classified image(nrow, ncol) with two pixel values: 0=NotWater; 128=Water
        """

        import pickle, math

        # prepare predictive attributes from the raw input images
        X = compute_attributes(images)

        # load a classification model, may be trained and pickled already, or each time.
        # Todo: refactoring
        path2pickmodfile = "/g/data/u46/fxz547/Githubz/geodata_analytics/notebooks/detree_clf.pickle"

        with open(path2pickmodfile, 'rb') as f:
            # The protocol version used is detected automatically, so we do not
            # have to specify it.
            my_clf = pickle.load(f)

        y = my_clf.predict(X)

        #make sure y has value 0 or 128  tranform if necessary y[y=='Water']=128

        ncols= int( math.sqrt(y.size) )

        logging.debug(ncols)  # should be 4000

        classified = numpy.reshape(y, (-1, ncols))  #numpy.zeros((rows, cols), dtype='uint8')

        #OR y.shape = (y.size/ncols, ncols); return y # which is a 2D image now

        # use y to re-define the pixels values of the classified img

        return classified


##############################################################################################################
def band_ratio(a, b):
    """
    Calculates a normalised ratio index of the images a and b
    """
    c = (a - b) / (a + b)
    return c


#######################################################################
def compute_attributes(images):
    """
    compute designed feature_attributes from input images
    :param images: 6-band NBAR image(nrow, ncol)
    :return: 2D numpy array (nf, nrow * ncol), where nf is the number of attributes used in the model.
    """

    dims = images.shape
    if len(dims) == 3:
        bands = dims[0]
        rows = dims[1]
        cols = dims[2]
    else:
        rows = dims[0]
        cols = dims[1]

    # datype = images.dtype


    Xp = numpy.empty((rows*cols,3), dtype='float32')

    NDI_52 = band_ratio(images[4], images[1])
    # NDI_43 = band_ratio(images[3], images[2])
    NDI_72 = band_ratio(images[5], images[1])
    # and the SHortWave Infrared1 which is the band-5 for LS5/7; but band-6 for the LS8
    #SWI_1 = images[4]

    Xp[:,0] = NDI_52.flatten()
    Xp[:,1] =NDI_72.flatten()
    Xp[:,2] = images[4].flatten()

    return Xp



##########################################################################
# Original test driver for Norman Muller classifier
# -------------------------------------------------------------------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser(description='Executes a predefined decision tree.')
    parser.add_argument('--B1_Stack', required=True, help='Band 1 stack.')
    parser.add_argument('--B2_Stack', required=True, help='Band 2 stack.')
    parser.add_argument('--B3_Stack', required=True, help='Band 3 stack.')
    parser.add_argument('--B4_Stack', required=True, help='Band 4 stack.')
    parser.add_argument('--B5_Stack', required=True, help='Band 5 stack.')
    parser.add_argument('--B7_Stack', required=True, help='Band 7 stack.')
    parser.add_argument('--outfile', required=True, help='The output filename.')
    parser.add_argument('--driver', default='ENVI',
                        help="The file driver type for the output file. See GDAL's list of valid file types. (Defaults to ENVI).")

    parsed_args = parser.parse_args()

    B1_Stack = parsed_args.B1_Stack
    B2_Stack = parsed_args.B2_Stack
    B3_Stack = parsed_args.B3_Stack
    B4_Stack = parsed_args.B4_Stack
    B5_Stack = parsed_args.B5_Stack
    B7_Stack = parsed_args.B7_Stack
    outfile = parsed_args.outfile
    driver = parsed_args.driver

    # Open each dataset
    ds1 = gdal.Open(B1_Stack)
    ds2 = gdal.Open(B2_Stack)
    ds3 = gdal.Open(B3_Stack)
    ds4 = gdal.Open(B4_Stack)
    ds5 = gdal.Open(B5_Stack)
    ds7 = gdal.Open(B7_Stack)

    # Create a list of opened datasets
    datasets = [ds1, ds2, ds3, ds4, ds5, ds7]

    # Get the number of bands, columns, rows, spectral_bands
    nb = ds1.RasterCount
    rows = ds1.RasterYSize
    cols = ds1.RasterXSize
    sb = len(datasets)

    # Get the datatype
    band = ds1.GetRasterBand(1)
    dtype = gdal_to_numpy_dtype(band.DataType)

    # Setup the output dataset
    driver = gdal.GetDriverByName(driver)
    outds = driver.Create(outfile, cols, rows, nb, 1)  # Uint8
    outds.SetGeoTransform(ds1.GetGeoTransform())
    outds.SetProjection(ds1.GetProjection())
    outband = []
    for i in range(nb):
        outband.append(outds.GetRasterBand(i + 1))
        outband[i].SetNoDataValue(0)

    classifier = WaterClassifier()

    # Loop over each timeslice (number of bands per spectral band stack).
    for i in range(nb):
        images = numpy.zeros((sb, rows, cols), dtype=dtype)
        bground = numpy.zeros((rows, cols), dtype='bool')
        for j in range(len(datasets)):
            band = datasets[j].GetRasterBand(i + 1)
            images[j] = band.ReadAsArray()
            no_data = band.GetNoDataValue()
            bground |= images[j] == no_data
            band = None

        water_class = classifier.classify(images)
        water_class[bground] = 0
        outband[i].WriteArray(water_class)
        outband[i].FlushCache()

    # Close the output file
    outds = None

###################################################################################
