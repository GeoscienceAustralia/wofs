import os
from wofs.utils.timeparser import find_datetime
from bisect import bisect_left

class FuzzyTileFinder(object):
    """
    Wraps a directory containing files with timestamps in the filenames.
    Provides 'nearest time' access to these files
    """

    def __init__(self, path, func=find_datetime):
        """
        Construct a FuzzyTileFinder 

        :param path:
            path to a directory containing files with timestamp in filename

        :param func:
            a function returning the timestamp value given a filename
        """
        self.path = path
        self.func = func
        self.entries = []
        self._load_entries()
    
    class Entry(object):
        """
        Represents a file with associated timestamp value
        """
        def __init__(self, parent, timestamp, filename):
            self.parent = parent
            self.timestamp = timestamp
            self.filename = filename

        def path(self):
            return os.path.join(self.parent.path, self.filename)

        def __str__(self):
            return "Entry: %s at %s" % (self.filename, self.timestamp)
  
        def __repr__(self):
            return str(self)

    def _load_entries(self):
        """
        Create a list of entries comprising all files in directory with
        valid timestamps
        """
        self.entries = []
        for f in os.listdir(self.path):
            try:
                ts = self.func(f)
                self.entries.append(FuzzyTileFinder.Entry(self, ts, f))
            except:
                pass   # there may be files with no timestamp
            
        # sort by timestamp

        self.entries.sort(key=lambda e: e.timestamp)

    def find_nearest(self, timestamp):
        """
        Find file nearest to supplied timestamp
        return associate Entry
        """
        if len(self.entries) == 0:
            return None
        times = [e.timestamp for e in self.entries]
        i = max(bisect_left(times, timestamp)-1, 0)
        return min(self.entries[i: i+2], key = lambda t: abs(timestamp - t.timestamp))
        
    
