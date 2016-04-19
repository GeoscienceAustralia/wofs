import matplotlib.animation as animation
import numpy as np
import matplotlib.pyplot as plt

# Only 2 required for this demo
from netCDF4 import Dataset, num2date, date2num
from ipywidgets import *

#f = Dataset('/g/data/rs0/tiles/EPSG3577/LS5_TM_NBAR/LS5_TM_NBAR_3577_15_-40_2006.nc', 'r')
f = Dataset('/Softdata/data/water_extents/149_-036_subset/py_stacked_CF.nc', 'r')

# The NC data variable name?
band4view='Data' #'band_2'
bandarray = f[band4view][:, :, :]  # this may use up memory error
#bandarray = f[band4view]

fig = plt.figure()

time=0
#im = plt.imshow(bandarray[time,:,:], cmap=plt.get_cmap('viridis'), animated=True)
im = plt.imshow(bandarray[time,:,:], cmap="Greys", animated=True)


def updatefig(*args):

    global time
    time += 1
    max_time=bandarray.shape[0]
    if (time >= max_time): time=0

    print time

    im.set_array(bandarray[time,:,:])
    return im,

ani = animation.FuncAnimation(fig, updatefig, interval=50, blit=False)
#blit is for smooth transition, False is display 1-by-1. interval= animimation play speed

plt.show()
