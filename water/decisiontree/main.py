from water.decisiontree import WaterClassifier
import numpy
import sys

def get_images(dataset_path):
    """
    read the input NBAR datasets (Bands) into an imager array to be feed into the classifier
    :param dataset_path:
    :return: numarray 6-band images 6x4000x4000
    """
    datatype= 'Int16'
    rows=4000
    cols=4000
    image  = numpy.random.random_integers(0,32767, size=(6, rows,cols))
    return image


if __name__ == '__main__':
# /g/data/v10/ZTEST/testdc/LS5_TM_NBAR/LS5_TM_NBAR_3577_10_-39_20060129001120781025.nc
# /g/data/v10/reprocess/ls5/nbar/packaged/2006/01/LS5_TM_NBAR_P54_GANBAR01-002_096_085_20060120/product


    in_data_path=sys.argv[1]

    images= get_images(in_data_path)


    classifier = WaterClassifier()
    water_class = classifier.classify(images)

    print ("output the classified image file")



