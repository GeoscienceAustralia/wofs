'''
Created on 27/03/2013

@author: Steven Ring
'''

import numpy
import numexpr
from water_constants import *



class WaterBand(object):
    """A wrapper class for the a water band pixel array. The value of the pixels confirms
       to the definitions found in WaterConstants.py. Convenience methods supply information
       about the water band pixel array.

       TODO: refactor this class into a WaterExtent class, possibly eliminating the WaterTile
       class above
    """

    def __init__(self, aWaterBandArray):
        """Construct a WaterBand encapsulating the supplied WaterBandArray
        """
        self._pixelData = aWaterBandArray

    def getPixelData(self):
        return self._pixelData

    def getHistogram(self):
        """Generate the histogram from the pixel data
        """
        (counts, bins) = numpy.histogram(self.getPixelData(), range(0,256))
        return (counts, bins)

    def printHistogram(self):
        (counts, bins) = self.getHistogram()
        totPixels = 0
        for i in range(0,len(bins)-1) :
            if counts[i] > 0 :
                print "%s: %d" % (getCategoryDescription(bins[i]), counts[i])
                totPixels += counts[i]
        print "Total pixels = %d" % totPixels

    def checkStats(self):
        """Do an internal integrity check of the WaterBand
        """
        (counts, bins) = self.getHistogram()
        totPixels = 0
        for i in range(0,len(bins)-1) :
            if counts[i] > 0 :
                totPixels += counts[i]
        assert totPixels == (4000*4000), "Expected 16,000,000 pixels but got %d" % totPixels

    def getNonObservations(self):
        wb = self.getPixelData()
        return numexpr.evaluate("wb < WATER_PRESENT and wb > WATER_NOT_PRESENT")

    def getNonObservationsCount(self):
        return numpy.sum(self.getNonObservations())

    def getWaterPixels(self):
        wb = self.getPixelData()
        return numexpr.evaluate("wb == WATER_PRESENT")

    def getWaterPixelCount(self):
        return numpy.sum(self.getWaterPixels())

    def getDryPixels(self):
        wb = self.getPixelData()
        return numexpr.evaluate("wb == WATER_NOT_PRESENT")

    def getDryPixelCount(self):
        return numpy.sum(self.getDryPixels())

    def getStatistics(self):
        """Return a dictionary containing the pixel count statistics relating to this WaterBand
        """
        result = {}
        (counts, bins) = self.getHistogram()
        for i in range(0,len(bins)-1) :
            result[getCategoryKey(bins[i])] = counts[i]
        return result
