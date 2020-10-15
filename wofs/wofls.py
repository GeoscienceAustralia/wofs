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
from wofs.vp_wofs import _fix_nodata_to_single_value

from wofs import classifier, filters
from wofs.constants import NO_DATA
from wofs.filters import eo_filter, fmask_filter, terrain_filter


def woffles(nbar, pq, dsm):
    """Generate a Water Observation Feature Layer from NBAR, PQ and surface elevation inputs."""

    water = classifier.classify(nbar.to_array(dim='band')) \
            | filters.eo_filter(nbar) \
            | filters.pq_filter(pq.pqa) \
            | filters.terrain_filter(dsm, nbar)

    _fix_nodata_to_single_value(water)

    assert water.dtype == np.uint8

    return water
