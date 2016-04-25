import re
from datetime import datetime

DT_PAT = re.compile( \
    "(?:.*)" + \
    "(?P<year>\\d{4})" + "-" + \
    "(?P<month>\\d{2})" + "-" + \
    "(?P<day>\\d{2})" + "(?:T|\\s+)" + \
    "(?P<hrs>\\d{2})" + "(?::|-)" + \
    "(?P<mins>\\d{2})" + "(?::|-)" + \
    "(?P<secs>\\d{2})" + \
    "\\.?(?P<usecs>\d+)?" +
    "(?:.*)")


def find_datetime(dt_string):
    """
    Search the supplied string for a "ISO8601-like" datetime 
    and return equivalent datetime object

    :param dt_string:
       String containing an ISO8601 datetime string or 
       some close approximation

    :return:
       A naive datetime instance corresponding to a matching
       datetime found in the dt_string, or None if no datetime
       is found
    """

    dt = None
    m = DT_PAT.match(dt_string)
#    print "dt_string=", dt_string
#    print "m=", str(m)
    if m is not None:
#        print m.groupdict()
        usecs = 0
        if m.group('usecs') is not None:
            usecs = int(m.group('usecs'))
        dt = datetime( \
            int(m.group('year')),
            int(m.group('month')),
            int(m.group('day')),
            int(m.group('hrs')),
            int(m.group('mins')),
            int(m.group('secs')),
            usecs)
    return dt
