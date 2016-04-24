#!/bin/env python
"""
Prepare for WOfS processing by creating a working directory and copying
key files to it.

Usage:

```
module load wofs
wofs_setup.py --base_dir PATH --run_id RUN_ID --run_desc "a description"
```
where:
   
# ``base_path`` must exist and
# ``run_id`` is any string which is a valid component of a directory name
# ``run_desc`` is description of the WOfS run

The program will read the file ``template_client.cfg`` which is expected to be
a sibling file to this script. It will then:

* read the template contents
* substitute the ``base_dir``, ``run_id`` and ``run_desc`` variable to create a
final content of ``client.cfg``
* create the working directory for this WOfS run (the path is determined from
the the config resulting from the previous step)
* write the ``client.cfg`` file to the working directory
* copy in the ``logging.cfg`` file to the working directory

This ``client.cfg`` file supplies important paramaters that controle the WOfS run. 
The file may be edited before continuing with the WOfS run.
"""

import os
import sys
from os.path import join as pjoin, dirname, exists
import argparse
import logging
#no need to couple with wofs to die!from wofs import die
# from wofs.utils import die  # a utils function
from string import Template
from ConfigParser import ConfigParser
from StringIO import StringIO

def die(msg):
    """ Correct UNIX behavour for a dying process
    """
    sys.stderr.write(msg + "\n")
    sys.exit(1)

def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('--base_dir', \
        help='The directory in which to create the WOfS working directory', \
        default='/g/data/u46/wofs')
    parser.add_argument('--run_id', help='unique id for the WOfS run', required=True)
    parser.add_argument('--run_desc', help='description of this WOfS run', \
        default="Insert run description here")
    return parser.parse_args()

def mkdir_if_not_exists(path):
    if not exists(path):
        os.makedirs(path)

if __name__ == '__main__':

    # check that there is a "template_client.cfg" sibling of this script file

    script_path = os.path.abspath(__file__)

    # bad Assumption: template_client.cfg in the same dir as this script.
    template_path = "%s/template_client.cfg" % os.path.dirname(script_path)
    if not exists(template_path):
        die("Template %s not found" % template_path)

    # read the template
    
    with open(template_path) as infile:
        template = Template(infile.read())

    # base_dir must exist

    args = parse_args() 
    run_id = args.run_id
    run_desc = args.run_desc
    base_dir = args.base_dir
    if not os.path.exists(base_dir):
        die("Base directory %s does not exist" % base_dir)
   
    # get config content from template

    config_content = template.substitute(args.__dict__)

    # parse the config to get details

    config = ConfigParser()
    config.readfp(StringIO(config_content)) 

    # create the working directory

    log_cfg_path = config.get('core', 'logging_conf_file')
    work_path = config.get('wofs', 'working_dir')
    if exists(work_path):
        die("Error: the directory %s exists already! Remove the dir OR change your run_id, and re-run this init script." % (work_path, ))

    os.mkdir(work_path)

    # write the config file
        
    to_path = "%s/client.cfg" % (work_path, )
    with open(to_path, "w") as outfile:
        outfile.write(config_content)

    # copy the logging.cfg if it doesn't exist

    if not exists(log_cfg_path):
        template_path = "%s/logging.cfg" % os.path.dirname(script_path)
        if not exists(template_path):
            die("%s not found" % template_path)
        with open(template_path,'r') as infile:
            with open(log_cfg_path,'w') as outfile:
                outfile.write(infile.read())

    # all done

    print ("WOFS initialisation is completed. Please check and tune %s/client.cfg before running workflow further"%(work_path) )
