from water.decisiontree import WaterClassifier
import numpy
import os, sys
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
        ret_img= [generate_random_img(6,400,400) for i in range(3)]
    else:
        #readin image into (6 nrow X ncol)
        ret_img= readimg_from_nc(dataset_path)

    return ret_img

def generate_random_img(nb, rows,cols):
    #datatype= 'Int16'
    #rows=4000
    #cols=4000
    image  = numpy.random.random_integers(1,32767, size=(6, rows,cols))
    return image

def readimg_from_nc(ncfile):
    """
    read the it-th tile from the netcdf file containing multiple 6-band-images, or timesliced tiles
    :param ncfile:
    :return: 6 x Row X Col   (6-band 2D-image)

    """
    ncdat = Dataset(ncfile, 'r')
    b1=ncdat.variables['band_1']
    b2=ncdat.variables['band_2']
    b3=ncdat.variables['band_3']
    b4=ncdat.variables['band_4']
    b5=ncdat.variables['band_5']
    b6=ncdat.variables['band_7']

    (ntiles, nrow, ncol) = b1.shape

    print (ntiles, nrow, ncol)
    datatype=b1.datatype
    print datatype

    for it in range(ntiles):
        print it
         #initialize a 6XrowXco
        #images  = numpy.zeros((6,nrow,ncol), dtype=datatype)
        images  = numpy.ones((6,nrow,ncol), dtype=datatype)

        print images.shape, type(images)

        images[0,:,:] = b1[it, :, :]
        images[1,:,:] = b2[it, :, :]
        images[2,:,:] = b3[it, :, :]
        images[3,:,:] = b4[it, :, :]
        images[4,:,:] = b5[it, :, :]
        images[5,:,:] = b6[it, :, :]

        yield images



if __name__ == '__main__':
#python detree//main.py /g/data/rs0/tiles/EPSG3577/LS5_TM_NBAR/LS5_TM_NBAR_3577_15_-40_2006.nc
# 1 tile:    /g/data/v10/ZTEST/testdc/LS5_TM_NBAR/LS5_TM_NBAR_3577_10_-39_20060129001120781025.nc
# 88 tiles:  /g/data/rs0/tiles/EPSG3577/LS5_TM_NBAR/LS5_TM_NBAR_3577_15_-40_2006.nc
# Scenes:    /g/data/v10/reprocess/ls5/nbar/packaged/2006/01/LS5_TM_NBAR_P54_GANBAR01-002_096_085_20060120/product

    if (len(sys.argv)<2):
        in_data_path=None
    else:
        in_data_path=sys.argv[1]

    #images= get_images() # API test with a random image
    images= get_images(in_data_path)


    classifier = WaterClassifier()

    icounter=0
    for im in images:
        water_classified_img = classifier.classify(im)

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

    #  save the image to a file
    # https://www.google.com.au/webhp?sourceid=chrome-instant&ion=1&espv=2&ie=UTF-8#q=write%20numpy%20ndarray%20to%20file

        outfilename="waterextent%s.npy"%(icounter)
        path2outf=os.path.join("/tmp", outfilename)
        water_classified_img.tofile(path2outf)
        icounter += 1


        # plt.imshow(water_classified_img) # , cmap='Greys')
        # plt.show()




