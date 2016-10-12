
from __future__ import absolute_import, print_function
from datacube import Datacube

from wofs.wofls import woffles


def test_woffles():
    dc = Datacube(app='test_wofls')

    query = dict(lat=(-35.2950, -35.2951), lon=(149.1373, 149.1377))  # 2x2 patch, Middle of Lake Burley Griffith
    time = ('2016-01-10', '2016-01-14')
    nbar = dc.load(product='ls8_nbar_albers', time=time, **query)
    pq = dc.load(product='ls8_pq_albers', time=time, **query)
    dsm = dc.load(product='dsm1sv10', **query)

    print(nbar, pq, dsm)
    water = woffles(nbar, pq, dsm)

    assert water
