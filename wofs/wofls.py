"""
Produce water observation feature layers (i.e. water extent foliation).

These are the WOfS product with temporal extent (i.e. multiple time values).
Consists of wet/dry estimates and filtering flags, 
with one-to-one correspondence to earth observation layers.

Not to be confused with the wofs summary products,
which are derived from a condensed mosaic of the wofl archive.

Issues:
    - previous documentation may be ambiguous or previous implementations may differ
      (e.g. saturation, bitfield)
    - Tile edge artifacts concerning cloud buffers and cloud or terrain shadows.
    - DSM may have different natural resolution to EO source.
      Should think about what CRS to compute in, and what resampling methods to use.
      Also, should quantify whether earth's curvature is significant on tile scale.
    - Yet to profile memory, CPU or IO usage.
"""


import numpy as np
import classifier_josh as classifier
import filters
from boilerplate import wofloven as boilerplate


@boilerplate(#lat=(-30.0, -30.1),#-31.0),
             #lon=(147.0,147.1),##148.0),
             time=('1992-08-01','1992-09-10'))#('2016-05-01','2017-01-01'))
def woffles(source, pq, dsm):
    """Generate a Water Observation Feature Layer from NBAR, PQ and surface elevation inputs."""

    water = classifier.classify(source.to_array(dim='band').data) \
            | filters.eo_filter(source) \
            | filters.pq_filter(pq.pixelquality.data) \
            | filters.terrain_filter(dsm, source)

    assert water.dtype == np.uint8

    return water

