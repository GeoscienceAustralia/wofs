"""
WOfS (wofl) product specification
=================================

Each value in a ``wofl`` indicates whether it contains a valid water determination,
and if it is not valid, why it has been excluded.

The clear and valid observations are:

clear dry == 0 or 4

clear wet == 128 or 132

===  =============  ==========  =======
Bit  Decimal value  Value       Meaning
===  =============  ==========  =======
0    0              0           no water present
0    1              1<<0        nodata (missing all earth observation bands)
1    2              1<<1        noncontiguous (missing any EO bands) or oversaturated/undersaturated
2    4              1<<2        sea
3    8              1<<3        terrain shadow or low solar angle
4    16             1<<4        high slope
5    32             1<<5        cloud shadow
6    64             1<<6        cloud
7    128            1<<7        wet*
===  =============  ==========  =======

The land/sea mask (bit 2) should be ignored. It is based on a vector sea mask
which excludes useful data. We are interested in keeping ocean observations
anyway.
"""

# pylint: disable=bad-whitespace, line-too-long

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

LOW_SOLAR_INCIDENCE_THRESHOLD_DEGREES = 30  # If the sun only grazes a hillface, observation unreliable (vegetation shadows etc)

