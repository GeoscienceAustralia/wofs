from wofs.utils.fuzzy_tile_finder import FuzzyTileFinder
from wofs.utils.tools import  phase_annee
from wofs.utils.timeparser import find_datetime
from datetime import datetime
import luigi
import os

class SingleUseLocalTarget(luigi.LocalTarget):
    """
    Represents a local file that should be deleted after it is processed by
    a Luigi Task. 
    """
    pass


def rm_single_use_inputs_after(cls):
    """
    Class decorator to remove input files after a run. Inspired by 
    Dale Roberts.

    Annotate any class that consumes SingleUseLocalTarget as inputs as 
    shown in this example.

    ```
    @rm_single_use_inputs_after
    class MyTask(luigi.Task):
        ...
    ```

    The SingleUseLocalTargets will be deleted once the run() method
    has finished
    """
    def decorate(fn):
        def rm_inputs_after_run(self):
            ret = fn(self)
            for f in self.input():
                if isinstance(f, SingleUseLocalTarget):
                   os.unlink(f.path)
            return ret
        return rm_inputs_after_run
    cls.run = decorate(cls.run)
    return cls



class FuzzyTileTarget(luigi.LocalTarget):
    """
    A LocalTarget represents a file on disk specified precisely by
    the path property. The exists() method of a LocalTarget tests for
    the existance of the file explicitly defined by the path.

    By contast, a FuzzyTileTarget allows a file "nearby" the nominal path
    to satisfy the existance test. 

    The "nearby" test relies on 

        1. a collection of candidate files within a single directory
        2. a timestamp embedded within with each file name
        3. function to return the timestamp value given a filename, and
        4. a delta value representing the maximum permissible difference
           between a nominal timestamp value and an actual file timestamp
    
    """

    def __init__(self, nominal_path, delta, func=find_datetime):
        """
        Create a FuzzyTileTarget

        :param nominal_path:
            the path of the tile with embedded timestamp

        :param delta:
            the maximum allowed diffence between the nominal_path
            timestamp and the timestamp found in "nearby" files

        :param func:
            a function which returns a timestamp give a filename
        """
        super(FuzzyTileTarget, self).__init__(nominal_path)
        self.delta = delta
        self.func = func
        self.timestamp = func(nominal_path)

    def exists(self):
        """
        Test for the existance of the file specified by the nominal_path
        or something with delta time units of the nominal_path
        """
        ff = FuzzyTileFinder(os.path.dirname(self.path), func=self.func)
        entry = ff.find_nearest(self.timestamp)
        if entry is not None:
            diff = self._get_diff(self.timestamp, entry.timestamp)
            if diff <= self.delta:
                return True
        return False

    def nearest_path(self):
        """
        Return the path to the file nearest to the nominal path 
        (but with timestamp within delta). If no actual file
        satisfies this criteria, then return the nominal path
        """
        ff = FuzzyTileFinder(os.path.dirname(self.path), func=self.func)
        entry = ff.find_nearest(self.timestamp)
        if entry is not None:
            diff = self._get_diff(self.timestamp, entry.timestamp)
            if diff <= self.delta:
                return entry.path()
        return self.path

       
    def _get_diff(self, a, b):
        """
        Compute the differnence between a and b 
        (which may datetime instances or simple numeric types)
        """
        if isinstance(a, datetime):
            return abs(a - b).total_seconds() 
        return abs(a - b)
        
        
    

class FuzzyShadowTileTarget(FuzzyTileTarget):


    def __init__(self, nominal_path, delta):
        """
        Create a FuzzyShadowTileTarget

        Shadow tiles (Ray-traced shadow masks and solar incident angle tiles)
        have their timestamps converted to an "annual phase"
        which expresses the datetime as a fraction of the year elapsed.

        Tiles with idential "annual phase" but from diffent years will have
        identical shading characteristics because the sun will have the same
        altitude and azimuth.

        :param nominal_path:
            the path of the tile with embedded timestamp

        :param delta:
            the maximum allowed diffence between the nominal_path
            timestamp and the timestamp found in "nearby" files
        """
        super(FuzzyShadowTileTarget, self).__init__(nominal_path, delta, \
            func=lambda f: phase_annee(find_datetime(f)))
