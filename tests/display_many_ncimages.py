"""
Display a list of images 1-by-1 animated
"""

import matplotlib.animation as animation
import numpy as np
import matplotlib.pyplot as plt

# Only 2 required for this demo
from netCDF4 import Dataset, num2date, date2num
from ipywidgets import *

def updatefig(*args):

    global time
    time += 1
    max_time=60
    if (time >= max_time): time=0

    print time
    path2ncimage="/g/data1/u46/fxz547/wofs2/extents/waterextent%s.nc"%(str(time))
    f = Dataset(path2ncimage, 'r')

# The NC data variable name?
    band4view='waterextent%s.nc'%(str(time))
    bandarray = f[band4view][:, :]  # this will load all data into RAM - may use up memory, spit out error

    im.set_array(bandarray[:,:])
    return im,


time=1
path2ncimage="/g/data1/u46/fxz547/wofs2/extents/waterextent%s.nc"%(str(time))
f = Dataset(path2ncimage, 'r')

# The NC data variable name?
band4view='waterextent%s.nc'%(str(time))
bandarray = f[band4view][:, :]  # this will load all data into RAM - may use up memory, spit out error

fig = plt.figure()

#im = plt.imshow(bandarray[time,:,:], cmap=plt.get_cmap('viridis'), animated=True)
im = plt.imshow(bandarray[:,:], cmap="Greys", animated=True)



ani = animation.FuncAnimation(fig, updatefig, interval=50, blit=False)
#blit is for smooth transition, False is display 1-by-1. interval= animimation play speed

plt.show()
