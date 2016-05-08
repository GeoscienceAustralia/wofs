#!/bin/env python
"""
Purpose:    Create a working directory and set up configuration files for the WOfS system to use in processing.

Input:      User input path2wofs.yml
Output:     A Working dir and client.cfg logging.cfg
Assumption: A default template_client.cfg file exists as a sibling file to this script.
            Savy user or Developer can override this file by supplying a similar-formatted template config file in the commandline

Usage:      python wofs_setup.py path2wofs.yml [optional_byo_template_client.cfg]

Process Details:
    The program will read the file wofs_input.yml and ``template_client.cfg`` which
    * read the template contents
    * substitute the base_dir, run_id, etc variables to create a final content of ``client.cfg``
    * create the working directory for this WOfS run
    * write the client.cfg file to the working directory
    * copy in the ``logging.cfg`` file to the working directory

    This client.cfg file supplies important parameters that control the WOfS run by Luigi
    http://luigi.readthedocs.io/en/stable/configuration.html
    The file may be edited before further processing with the WOfS.
"""

import os
import sys
import shutil
from datetime import datetime
import yaml
import logging

from string import Template
#https://www.python.org/dev/peps/pep-0292/
from ConfigParser import ConfigParser
from StringIO import StringIO


def mkdir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        pass


logging.basicConfig()
_logger = logging.getLogger(__file__)  # (__name__) ()is root
_logger.setLevel(logging.INFO)
_logger.setLevel(logging.DEBUG)


class WofsSetup:
    """
    WOfS process initialization setup
    """

    def __init__(self, path2yamlfile):

        self.yamlfile = path2yamlfile

        return

    def write2file(self, configdict, outfile):
        # configdict = {'key1': 'value1', 'key2': 'value2'} nested

        with open(outfile, 'w') as f:
            yaml.dump(configdict, f, default_flow_style=False)

    def loadyaml(self):
        """
        Read data from an input yaml file, which contains wofs run parameters
        :return: a dictionary representation of the yaml file
        """

        with open(self.yamlfile, 'r') as f:
            indict = yaml.safe_load(f)

        # now we got a dictionary: config
        _logger.debug(indict.get('run_id'))

        _logger.debug(yaml.dump(indict, None, default_flow_style=False))  # onto screen

        return indict

    def generate_runid(self):
        """
        generate a run_id
        :return:
        """

        runuser = os.environ['USER']  # _datetime stamp
        dtstamp = datetime.today().isoformat()[:19].replace(':', '-')

        runid = "%s_%s" % (runuser, dtstamp)
        _logger.debug(runid)

        return runid

    def main(self, template_conf_file=None):
        """ main method to do the setup for a wofs run.

        :param template_conf_file: path to an template configuration file for WofS process.
        If none, will try to find "template_client.cfg" as a sibling file of this script file.
        The client.cfg file will be generated and written into working dir.

        :return: path2workingdir
        """

        # read in the user input parameter from a yaml file, store as a dict:
        inputdict = self.loadyaml()

        # Sanity-check the user inputs and massage them for subsequent use.
        run_id = inputdict.get('run_id')
        if run_id is None:
            # generate run_id
            inputdict['run_id'] = self.generate_runid()

        #If needed, redefine the start_datetime to: 2016-01-01T00:00:00Z

        # system-defined template conf file
        if (template_conf_file is None):

            # default to a "template_client.cfg" as sibling of this script file
            script_path = os.path.abspath(__file__)
            template_path = "%s/template_client.cfg" % os.path.dirname(script_path)
        else:
            template_path = template_conf_file

        if not os.path.exists(template_path):
            raise Exception("Template file %s not found" % template_path)

        # read the template

        with open(template_path) as infile:
            template = Template(infile.read())  # https://docs.python.org/2/library/string.html

        # # base_dir must exist
        # if not os.path.exists(base_dir):
        #     raise Exception("Base directory %s does not exist" % base_dir)

        # get config_content from the template substituted with user's input dict.
        config_content = template.substitute(inputdict)

        # parse the config to get details

        config = ConfigParser()  # https://docs.python.org/2/library/configparser.html
        config.readfp(StringIO(config_content))

        _logger.debug(config.get('wofs','extents_dir'))
        _logger.debug(config.get('wofs','sia_dir'))
        _logger.debug(config.get('wofs','tsm_dir'))


        # create the working directory
        work_path = config.get('wofs', 'working_dir')
        if os.path.exists(work_path):
            raise Exception(
                "Error: Directory %s already exists. Please remove it or change your run_id." % (work_path,))

        os.mkdir(work_path)

        # write the config file

        to_path = "%s/client.cfg" % (work_path)
        with open(to_path, "w") as outfile:
            outfile.write(config_content)

        debug_simple_conf=os.path.join(work_path,'simplified_client.cfg')
        with open(debug_simple_conf, 'wb') as configfile:
            config.write(configfile)

        template_log_cfg = "%s/logging.cfg" % os.path.dirname(script_path)
        if not os.path.exists(template_log_cfg):
            raise Exception("logging conf file %s not found" % template_log_cfg)

        # copy the logging.cfg into work dir
        work_log_cfg = os.path.join(work_path, 'logging.cfg')

        shutil.copy2(template_log_cfg, work_log_cfg)

        _logger.info("WOfS working dir has been created. Please check: \n %s " % (work_path))

        _logger.info(os.listdir(work_path))

        return work_path


#############################################################################
#
# Uasge:  python wofs_setup.py wofs_input.yml [path2/template_client.cfg]
#
#############################################################################
if __name__ == "__main__":

    template_client = None
    if len(sys.argv) < 2:
        print "Usage: %s %s %s" % (sys.argv[0], "path2/wofs_input.yml", "[path2/template_client.cfg]")
        sys.exit(1)
    elif len(sys.argv) == 3:
        template_client = sys.argv[2]

    wofsObj = WofsSetup(sys.argv[1])

    workdir = wofsObj.main(template_client)

