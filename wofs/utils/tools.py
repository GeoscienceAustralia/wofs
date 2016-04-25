import re
import os
from os.path import join as pjoin
import sys
from datetime import datetime
import dateutil.parser
#import wofs  # circular import stpd __init__.py
import argparse
import luigi
import logging
from datacube.api.model import Cell
import shutil
import numpy as np

CONFIG = luigi.configuration.get_config()
logger = logging.getLogger(__name__)

def datetime_from_nbar(nbar_filename):

    """
    Return the datetime of the supplied NBAR filename
    Raise a ValueError if there is no recognised datetime 
    pattern in the supplied filename

    Note, some datacube tiles do not have a microsecond
    component so we have to check two possible formats
    """

    # check for full date time with microseconds

    pat = "\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}.\d{1,6}"
    dt_pat = "%Y-%m-%dT%H-%M-%S.%f"
    m = re.search(pat, nbar_filename)
    if m is not None:
        return datetime.strptime(m.group(0), dt_pat)

    # check for full date time with no microseconds

    pat = "\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}"
    dt_pat = "%Y-%m-%dT%H-%M-%S"
    m = re.search(pat, nbar_filename)
    if m is not None:
        return datetime.strptime(m.group(0), dt_pat)

    raise ValueError("No datetime found in '%s'" % (nbar_filename, ))

def phase_annee(timestamp):
    """
    Compute the fraction of the year completed at the given timestamp
    """
    d0 = datetime(timestamp.year, 1, 1, 0, 0,0)
    d1 = datetime(timestamp.year+1, 1, 1, 0, 0,0)
    return (timestamp-d0).total_seconds()/(d1-d0).total_seconds()

def filename_from_datetime(dt_utc):
    """
    Return the timestamp string for filenames from the
    supplied datetime instance
    """
    utc = dt_utc
    if isinstance(dt_utc, str):
       utc = datetime_from_iso8601(dt_utc)

    if utc.microsecond == 0:
        return utc.strftime("%Y-%m-%dT%H-%M-%S")
    else:
        return utc.strftime("%Y-%m-%dT%H-%M-%S.%f")


def datetime_from_iso8601(iso8601_string):
    """
    Parse the supplied ISO8601 string and return the 
    resulting datetime instance
    """
    return dateutil.parser.parse(iso8601_string)

def die(msg):
    """
    Correct UNIX behavour for a dying process
    """
    sys.stderr.write(msg + "\n")
    sys.exit(1)

def mkdirs_if_not_present(path):
    """
    Create the specified directory, and parents as required,
    if they don't exist

    Guard against race condition that exists in HPC workflows
    where multiple threads may be trying to do the same thing
    """
    if os.path.exists(path):
        logger.debug("Bypass creation of existing path %s" % (path, ))
        # race condition here ... other process may create path
    else:
        try:
            os.makedirs(path)
            logger.debug("Created %s" % (path, ))
        except OSError, e:
            if "File exists" in str(e):
                logger.debug("Bypass path created in race conditon %s" % (path, ))
                pass
            else:
                logger.debug("Got exception creating %s: %s" % (path, str(e)))
                raise e
    return path

def get_work_path(config):
    return config.get('paths', 'working_dir')

def get_input_path(config):
    return "%s/%s" % (get_work_path(config), config.get('inputs', 'path'))
    

def get_config_path_from_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('--config_path', help='path to a config file', required=True)
    args = parser.parse_args()

    return args.config_path

def parse_args_and_config():

    parser = argparse.ArgumentParser()
    parser.add_argument('--config_path', help='path to a config file', required=True)
    parser.add_argument('--log_level', help='Set logging level (overrides config)', required=False)
    args = parser.parse_args()

    # use the config.cfg file in the working directory

    config_path = args.config_path
    if not os.path.isfile(config_path):
        die("%s is not a file" % (config_path, ))
    CONFIG.add_config_path(config_path)

    if args.log_level is not None:
        CONFIG.set('logs', 'level', args.log_level)
    return config_path


def setup_logging(log_dir_name, log_prefix):
    # setup logging from command line attributes

    CONFIG = luigi.configuration.get_config()
    log_dir = pjoin(CONFIG.get('wofs', 'logs_dir'), log_dir_name)
    mkdirs_if_not_present(log_dir)
    log_filename = "%s_%s_%d.log" % (log_prefix, os.uname()[1], os.getpid())
    log_path = "%s/%s" % (log_dir, log_filename)

    logging.basicConfig( \
        filename=log_path, \
        filemode="w", \
       #  format='%(asctime)s: [%(name)s] (%(levelname)s) %(message)s {RSS=%(rss_MB).1fMB,SWAP=%(swap_MB).1fMB}', \
        format='%(asctime)s: [%(name)s] (%(levelname)s) %(message)s', \
        level=getattr(logging,CONFIG.get('logs','level').upper(),logging.WARN))

def setup_logging_maybeusethis(log_dir_name, log_prefix):
    # add the custom filter to support memory status logging

    logging.root.handlers[0].addFilter(wofs.MemuseFilter())

    # setup logging from command line attributes

    log_name = os.path.basename(__file__).split('.')[0]
    log_dir = pjoin(CONFIG.get('paths', 'logs_dir'), log_name)
    mkdirs_if_not_present(log_dir)

    log_filename = "{log_name}_{host}_{pid}.log".format( \
        log_name=log_name, host=os.uname()[1], pid=os.getpid())
    log_path = pjoin(log_dir, log_filename)

    # fmt='%(asctime)s: [%(name)s] (%(levelname)s) %(message)s {RSS=%(rss_MB).1fMB,SWAP=%(swap_MB).1fMB}', \
    fmt='%(asctime)s: [%(name)s] (%(levelname)s) %(message)s'
    logging.basicConfig( \
        filename=log_path, \
        filemode="w", \
        format=fmt,
        level=getattr(logging,CONFIG.get('logs','level').upper(),logging.WARN))



def get_input_cell_list(input_path):
    """
    Return a list of cells found in the input path. A cell file in the input_path directory has a filename
    like ``cell_<lon>_<lat>_tiles.csv``
    """
    cells = []
    p = re.compile("cell_(-?\d*)_(-?\d*)_tiles.csv")
    for fname in os.listdir(input_path):
        m = p.match(fname)
        if m is not None:
            x = int(m.group(1))
            y = int(m.group(2))
            cells.append(Cell(x, y))
    return cells
 
def scatter(iterable, P=1, p=1):
    """
    Scatter an iterator across `P` processors where `p` is the index
    of the current processor. This partitions the work evenly across
    processors.
    """
    import itertools
    return itertools.islice(iterable, p-1, None, P)

def get_cell_temp_dir(x, y):
    """
    Return the full path to a temporary directory to be used as a node-local-disk scratch area
    Use the current working directory if PBS_JOBFS is not specified
    """
    temp_dir = pjoin(os.getenv('PBS_JOBFS', "."), "%03d_%04d" % (x, y))
    mkdirs_if_not_present(temp_dir)
    return temp_dir

def delete_cell_temp_dir(x, y):
    """
    Delete the cell scratch directory 
    Use the current working directory if PBS_JOBFS is not specified
    """
    temp_dir = pjoin(os.getenv('PBS_JOBFS', "."), "%03d_%04d" % (x, y))
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    return temp_dir

def compute_percent_seen(feature_count, observation_count, scale_factor=1.0):

    divisor = np.where(observation_count == 0, 1.0, observation_count * 1.0)
    m = 100.0 * scale_factor
    numerator = feature_count * m
    return np.true_divide(numerator, divisor).astype(np.float32)


def collect_config_values(config, section_list):
    """
    Collection all config values from the sections listed in the section list
    

    :param config:
        A ConfigParser instance

    :param section_list:
        A list of section names

    :return:
        A dictionary containing the key/value pairs found across all the 
        sections specified in section_list. Sections are processed in order
        within section_list. Where a key is found in multiple sections, 
        the value in the last section overrides the value in earlier section(s)
    """

    result = {}
    for section in section_list:
        result.update(config.items(section))
    return result

    
