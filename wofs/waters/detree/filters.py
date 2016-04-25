# A collection of classes designed to support 
# Pixel Quality filtering of the the HFMS 
# water bands produced by the water classifier
# (WaterFilter)
#
# @Author: Steven Ring

import numpy
import numexpr
from wofs.waters.common.WaterConstants import WATER_NOT_PRESENT, WATER_PRESENT, NO_DATA, MASKED_NO_CONTIGUITY, MASKED_CLOUD, MASKED_CLOUD_SHADOW, \
    MASKED_TERRAIN_SHADOW, MASKED_HIGH_SLOPE, MASKED_SEA_WATER
from scipy import ndimage

VERSION = 'n'

class Filter(object):
    """ Abstract superclass representing common behaviour
        and properties of a collaborating set of filters

        Subclasses of Filter implement the apply() method
    """

    PQA_CONTIGUITY_BITS = 0x01FF
    PQA_CLOUD_BITS = 0x0C00
    PQA_CLOUD_SHADOW_BITS = 0x3000
    PQA_SEA_WATER_BIT = 0x0200

    @staticmethod
    def getVersion():
        return VERSION

    def __init__(self):
        """Construct a Filter that will operate on a WaterExtent band
        """
        pass

    def getNoObservationsMask(self, water_band):
        """
        Compute No Observation mask based upon the supplied water band
        """
        return numexpr.evaluate("(water_band < WATER_PRESENT) & (water_band > WATER_NOT_PRESENT)")

    def demask_existing_no_obs(self, water_band, mask):
        """
        Find existing non-observations in water_band and remove them from mask
        """
        no_obs = self.getNoObservationsMask(water_band)
        return numexpr.evaluate("mask & (~ no_obs)")
          
    def apply(self, wb):
        """Apply the policies of this filter to the associated WaterBand
           This abstract method must be overriden by each concrete Filter
        """
        assert False, "No apply() method provided in Filter subclass"

class NoDataFilter(Filter):
    """
    A pixelQualityFilter that expects the supplied WaterBand to contain
    pixels with the NO_DATA bit set.  The NO_DATA class is superior to 
    all otehr noObservation categories
    """
      
    def __init__(self):
        super(NoDataFilter, self).__init__()

    def apply(self, water_band, nbar_bands, nodata_value):
        """
        Look for nodata_values in original NBAR bands
        """
        mask = numpy.any(nbar_bands == nodata_value, axis=0)
        mask = self.demask_existing_no_obs(water_band, mask)
        water_band[mask] = NO_DATA
        return water_band

class ContiguityFilter(Filter):
    """
    A Filter that perform data contiguity checks on a WaterBand
    """
      
    def __init__(self, pq_band):
        super(ContiguityFilter, self).__init__()
        self.pq_band = pq_band

    def apply(self, water_band):
        """
        Use the supplied PQ band to mask out pixels with non-contiguous data
        """

        # no op if no PQA mask is supplied
        if self.pq_band is None:
            return water_band

        # first remove unwanted bits of the PQ band
        mask = numpy.bitwise_and(self.pq_band, Filter.PQA_CONTIGUITY_BITS)
  
        # now pick out any contiguity bits that are turned OFF
        mask = numpy.bitwise_xor(mask, Filter.PQA_CONTIGUITY_BITS)
        mask = numexpr.evaluate("mask > 0")

        # mask the waterBand pixels which have non-contiguous data AND 
        # have not been masked before
        mask = self.demask_existing_no_obs(water_band, mask)
        water_band[mask] = MASKED_NO_CONTIGUITY
        return water_band


class CloudAndCloudShadowFilter(Filter):
    """
    A Filter that masks all pixels in a WaterBand that have been identified in a PQA band
    as being either cloud or cloud shadow. A dialation is performed prior 
    to applying the mask. This PQF handles both cloud and cloud shadow
    """
      
    def __init__(self, pq_band):
        super(CloudAndCloudShadowFilter, self).__init__()
        self.pq_band = pq_band

    def apply(self, water_band):
        """Use the supplied PQA band to mask out pixels which are classified as 
           cloud or cloud shadow 
        """

        # no op if no PQA mask is supplied
        if self.pq_band is None:
            return water_band

        # dilate the cloud and cloud shadow
        dilatedPQA = self.dialateCloudAndCloudShadow(self.pq_band)

        # first remove non-cloud bits of the PQA band
        mask = numpy.bitwise_and(dilatedPQA, 0x0C00)
  
        # now pick out any cloud bits that are turned OFF
        # these represent cloudy pixels
        mask = numpy.bitwise_xor(mask, 0x0C00)
        mask = numexpr.evaluate("mask > 0")

        # mask the waterBand pixels which have cloud AND 
        # have not been masked before
        mask = self.demask_existing_no_obs(water_band, mask)
        water_band[mask] = MASKED_CLOUD

        # now do cloud shadow
        # ===================

        # first remove non-cloud-shadow bits of the PQA band
        mask = numpy.bitwise_and(dilatedPQA, 0x3000)
        del dilatedPQA
 
        # now pick out any cloud shadow bits that are turned OFF
        # these represent cloud shadow pixels
        mask = numpy.bitwise_xor(mask, 0x3000)
        mask = numexpr.evaluate("mask > 0")

        # mask the waterBand pixels which have cloud shadow AND 
        # have not been masked before
        mask = self.demask_existing_no_obs(water_band, mask)
        water_band[mask] = MASKED_CLOUD_SHADOW
        return water_band

    def dialateCloudAndCloudShadow(self, pqa_array, n=3, dilation=3) :
        """
        Apply an n X n dilation to the cloud and cloud shadow regions of the
        supplied pixel quality mask and return the result
        """

        s = [[1]*n]*n
        acca = (pqa_array & 1024) >> 10
        erode = ndimage.binary_erosion(acca, s, iterations=dilation, border_value=1)
        dif = erode - acca
        dif[dif < 0] = 1
        pqa_array += (dif << 10)
        del acca
        fmask = (pqa_array & 2048) >> 11
        erode = ndimage.binary_erosion(fmask, s, iterations=dilation, border_value=1)
        dif = erode - fmask
        dif[dif < 0] = 1
        pqa_array += (dif << 11)
        del fmask
        acca_shad = (pqa_array & 4096) >> 12
        erode = ndimage.binary_erosion(acca_shad, s, iterations=dilation, border_value=1)
        dif = erode - acca_shad
        dif[dif < 0] = 1
        pqa_array += (dif << 12)
        del acca_shad
        fmask_shad = (pqa_array & 8192) >> 13
        erode = ndimage.binary_erosion(fmask_shad, s, iterations=dilation, border_value=1)
        dif = erode - fmask_shad
        dif[dif < 0] = 1
        pqa_array += (dif << 13)

        return pqa_array
          

class TerrainShadowFilter(Filter):
    """
    A Filter that masks all pixels in a WaterBand that have been identified as being
    in terrain shadow. The shadow mask is supplied when the the apply() method is called.
    """
      
    def __init__(self, shadow_mask):
        super(TerrainShadowFilter, self).__init__()
        self.shadow_mask = shadow_mask


    def apply(self, water_band):
        """
        Apply the shadow_mask to the supplied water_band. Pixels which are classified as 
        being in terrain shadow are set to MASKED_TERRAIN_SHADOW. 
        A terrain shadow_ban contains values ranging from 
        0 = full shadow to 255 = full sunlight
        """

        # no op if no shadow band is supplied
        if self.shadow_mask is None:
            return water_band

        # mask the waterBand pixels which are in shadow AND 
        # have not been masked before
        mask = numpy.copy(self.shadow_mask)
        mask = numexpr.evaluate("mask < 255")
        mask = self.demask_existing_no_obs(water_band, mask)
        water_band[mask] = MASKED_TERRAIN_SHADOW
        return water_band

class LowSolarIncidenceFilter(Filter):
    """
    A Filter that masks all pixels in a supplied WaterBand
    that have been identified as having a low solar incident angle relative to the slope.
  
    """
    def __init__(self, solar_incident_deg_band, threshold_deg=30):
        super(LowSolarIncidenceFilter, self).__init__()
        self.solar_incident_deg_band = solar_incident_deg_band
        self.threshold_deg = threshold_deg

    def apply(self, water_band):
        """
        Use the previously supplied solar_incident_deg_band to mask pixels in the supplied water_band
        which are classified as having low solar incidence angle. The solar_incident_deg_band contains
        values ranging from 0 to 90 degrees representing the anlge of solar incidence to the tangent plane
        of the slope. Pixels with angles below the threshold_deg value are masked out.
        """

        # no op if no shadow band is supplied
        if self.solar_incident_deg_band is None:
            return water_band

        # this is a no-op if thresholdDegrees is zero
        if self.threshold_deg == 0 :
            return water_band

        # mask the water_band pixels which have low solar incidence (below thresholdDegrees) AND 
        # have not been masked before
        solar_incident_deg_band = self.solar_incident_deg_band
        threshold_deg = self.threshold_deg
        mask = numexpr.evaluate("solar_incident_deg_band < threshold_deg")
        mask = self.demask_existing_no_obs(water_band, mask)
        water_band[mask] = MASKED_TERRAIN_SHADOW
        return water_band

    def getLowSolarIncidenceLimitDegrees(self):
        return self.threshold_deg

class HighSlopeFilter(Filter):
    """
    A Filter that masks all pixels in a WaterBand
    that have been identified as being on terrain where the slope exceeds
    some threshold.
    """
      
    def __init__(self, slope_band, slope_limit_degrees=12.0):
        super(HighSlopeFilter, self).__init__()
        self.slope_band = slope_band
        self.slope_limit_degrees = slope_limit_degrees


    def apply(self, water_band):
        """Use the supplied SlopeBand to mask out pixels which are classified as 
           being on a steep slope (based on the supplied slopeLimitDegrees).
        """

        # no op if no slope band is supplied
        if self.slope_band is None:
            return water_band

        # mask the waterBand pixels which are in shadow AND 
        # have not been masked before
        noObsMask = self.getNoObservationsMask(water_band)
        slope_band = self.slope_band
        limit_deg = self.slope_limit_degrees
        mask = numexpr.evaluate("(slope_band > limit_deg) & (~ noObsMask)")
        water_band[mask] = MASKED_HIGH_SLOPE
        return water_band

    def getSlopeLimitDegrees(self):
        return self.slope_limit_degrees

class SeaWaterFilter(Filter):
    """
    A Filter that modifies a water_band array by masking pixels
       that are flagged as Sea Water in the supplied pixel quality band
    """
      
    def __init__(self, pq_band):
        super(SeaWaterFilter, self).__init__()
        self.pq_band = pq_band


    def apply(self, water_band):
        """
        Use the supplied pq_band to mask out pixels in the supplied water_band
        that are flagged as Sea water
        """

        # no op if no PQA mask is supplied
        if self.pq_band is None:
            return water_band

        # first remove unwanted bits of the PQA band
        seaWaterBit = numpy.bitwise_and(self.pq_band, Filter.PQA_SEA_WATER_BIT)
  

        # mask the waterBand pixels which have non-contiguous data AND 
        # have not been masked before
        noObsMask = self.getNoObservationsMask(water_band)
        mask = numexpr.evaluate("(seaWaterBit == 0) & (~ noObsMask)")
        water_band[mask] = MASKED_SEA_WATER
        return water_band
