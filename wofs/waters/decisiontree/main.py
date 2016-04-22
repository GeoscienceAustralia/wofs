from water.decisiontree import WaterClassifier
import numpy
import sys
import matplotlib.pyplot as plt
#import matplotlib.image as mpimg

from netCDF4 import Dataset

from scipy import stats


def get_images(dataset_path=None):
    """
    read the input NBAR datasets (Bands) into an imager array to be feed into the classifier
    :param dataset_path: is None will use random generator
    :return: numarray 6-band 2D numpy array images 6x4000x4000
    """

    if dataset_path  is None:
        ret_img= generate_random_img(6,400,400)
    else:
        #readin image into (6 nrow X ncol)
        pass

    return ret_img

def generate_random_img(nb, rows,cols):
    #datatype= 'Int16'
    #rows=4000
    #cols=4000
    image  = numpy.random.random_integers(1,32767, size=(6, rows,cols))
    return image

def readimg_fromnc(ncfile):
    """
    read from a netcdf file containing 6-band 2D-images
    :param ncfile:
    :return: 6 x Row X Col   (6-band 2D-image)

    """
    ncdat=f = Dataset(ncfile, 'r')
    b1=ncdat.variables['band_1']
    b2=ncdat.variables['band_2']
    b3=ncdat.variables['band_3']
    b4=ncdat.variables['band_4']
    b5=ncdat.variables['band_5']
    b6=ncdat.variables['band_7']



if __name__ == '__main__':
# /g/data/v10/ZTEST/testdc/LS5_TM_NBAR/LS5_TM_NBAR_3577_10_-39_20060129001120781025.nc
# /g/data/v10/reprocess/ls5/nbar/packaged/2006/01/LS5_TM_NBAR_P54_GANBAR01-002_096_085_20060120/product

    in_data_path=sys.argv[1]

    images= get_images() # API test with a random image
    #images= get_images(in_data_path)


    classifier = WaterClassifier()
    water_classified_img = classifier.classify(images)

    print ("Verific the water classified image ")

    #verify that classified image is a 2D (4000X4000) 1 band image with possible values 0, 128, or -999?
    print water_classified_img.shape

    nowaterpix = numpy.sum(water_classified_img == 0)    # not water
    waterpix =numpy.sum(water_classified_img == 128)  #water
    nodatapix= numpy.sum(water_classified_img == -999)

    totatlpix=nowaterpix + waterpix + nodatapix

    print nowaterpix, waterpix, nodatapix, totatlpix

    wimg1d=water_classified_img.flat

    for i in range(0, len(wimg1d)):
        if (wimg1d[i] != 0) and (wimg1d[i] != 128):
            print i, wimg1d[i]

    print stats.describe(wimg1d)


    #plt.imshow(water_classified_img) # , cmap='Greys')
    #plt.show()




