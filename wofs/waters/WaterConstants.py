# Constants used in the Flood History Project
#
# @Author: Steven Ring, May 2013
#-------------------------------------------------------------

WATER_CONSTANTS_DOC = \
    """
    Byte value used in Water Extent files.  

    Note - legal (decimal) values are:

           0:  no water in pixel
           1:  no data (one or more bands) in source NBAR image
       2-127:  pixel masked for some reason (refer to MASKED bits)
         128:  water in pixel 

    Values 129-255 are illegal (i.e. if bit 7 set, all others must be unset)

    """

WATER_PRESENT         = 1 << 7   # (dec 128) bit 7: 1=water present, 0=no water if all other bits zero
MASKED_CLOUD          = 1 << 6   # (dec 64)  bit 6: 1=pixel masked out due to cloud, 0=unmasked
MASKED_CLOUD_SHADOW   = 1 << 5   # (dec 32)  bit 5: 1=pixel masked out due to cloud shadow, 0=unmasked
MASKED_HIGH_SLOPE     = 1 << 4   # (dec 16)  bit 4: 1=pixel masked out due to high slope, 0=unmasked
MASKED_TERRAIN_SHADOW = 1 << 3   # (dec 8)   bit 3: 1=pixel masked out due to terrain shadow or low solar incidence angle, 0=unmasked
MASKED_SEA_WATER      = 1 << 2   # (dec 4)   bit 2: 1=pixel masked out due to being over sea, 0=unmasked
MASKED_NO_CONTIGUITY  = 1 << 1   # (dec 2)   bit 1: 1=pixel masked out due to lack of data contiguity, 0=unmasked
NO_DATA               = 1 << 0   # (dec 1)   bit 0: 1=pixel masked out due to NO_DATA in NBAR source, 0=valid data in NBAR
WATER_NOT_PRESENT     = 0        # (dec 0)          All bits zero indicated valid observation, no water present


SLOPE_THRESHOLD_DEGREES = 12.0           # Water detected on slopes equal or greater than this value are masked out 

CUBE_PROJECTION = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,' \
                  'AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],' \
                  'UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]'

PIXEL_SIZE_DEGREES = 0.00025

def getCategoryDescription(value):
    if value == WATER_PRESENT:
        return "1a:wet_count"
    if value == MASKED_CLOUD:
        return "2c:cloud_count"
    if value == MASKED_CLOUD_SHADOW:
        return "2d:cloud_shadow_count"
    if value == MASKED_HIGH_SLOPE:
        return "2f:high_slope_count"
    if value == MASKED_TERRAIN_SHADOW:
        return "2e:terrain_shadow_count"
    if value == MASKED_SEA_WATER:
        return "2g:sea_water_count"
    if value == MASKED_NO_CONTIGUITY:
        return "2b:non_contiguity_count"
    if value == NO_DATA:
        return "2a:no_data_count"
    if value == WATER_NOT_PRESENT:
        return "1b:dry_count"
    return "3a:unknow_class_count"

def getCategoryKey(value):
    desc = getCategoryDescription(value)
    return desc.split(":")[1]
