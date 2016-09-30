"""
This file is not needed, but can be used for quick visualisation purposes.


Example:

>>>from demo_boilerplate import wofloven as boilerplate
>>>@boilerplate(time=.., lat=.., lon=..)
>>>def algorithm(*input_chunks):
>>>    return output_chunk

<output figure>

"""


bands = ['blue','green','red','nir','swir1','swir2']

platforms = ['ls8', 'ls7', 'ls5']
nbar_products, pq_products = zip(*[(p+'_nbar_albers', p+'_pq_albers') for p in platforms])


def wofloven(time, **extent):
    """Annotator for WOFL workflow""" 
    def main(core_func):
        print core_func.__name__

        import datacube
        dc = datacube.Datacube()


        source = dc.load(product=nbar_products[1], time=time, measurements=bands, **extent)
        print len(source.time)
        pq = dc.load(product=pq_products[1], time=time, **extent)
        print pq.time.values
        dsm = dc.load(product='dsm1sv10', output_crs=source.crs, resampling='cubic', resolution=(-25,25), **extent).isel(time=0)

        # produce results as 3D dataset
        import xarray
        ti = pq.time
        waters = xarray.concat((core_func(source.sel(time=t), pq.sel(time=t), dsm) for t in ti.values), ti).to_dataset(name='water')


        # visualisation
        import numpy as np
        import matplotlib.pyplot as plt
        import math
        n = len(pq.time)
        n1 = int(math.ceil(math.sqrt(n)))
        n2 = int(math.ceil(float(n)/n1))
        fig,axes = plt.subplots(n2,n1)
        for ax,t in zip(axes.ravel() if type(axes)==np.ndarray else [axes], pq.time.values):
            water = waters.sel(time=t).water
            background = source.sel(time=t).red.data            
            pretty = np.empty_like(water, dtype=np.float32)
            pretty[:,:] = np.nan
            pretty[water.data != 0] = 1 # red masking
            pretty[water.data == 128] = 0 # blue water
            a = ax.imshow(water) # for cursor data not display
            b = ax.imshow(background, cmap='gray')
            c = ax.imshow(pretty, alpha=0.4, clim=(0,1))
            b.get_cursor_data = a.get_cursor_data # bitfield on mouseover
            c.get_cursor_data = a.get_cursor_data
            ax.set_title(t)
        plt.show()


    return main

