"""
Setup
-----

"""

import codecs
import os
import re

from setuptools import setup, find_packages
from numpy.distutils.core import Extension, setup, Command

import versioneer

here = os.path.abspath(os.path.dirname(__file__))
config_files = ['config/' + name for name in os.listdir('config')]


def read(*parts):
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()


class PyTest(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import sys, subprocess
        errno = subprocess.call([sys.executable, 'runtests.py'])
        raise SystemExit(errno)


my_cmdclass = versioneer.get_cmdclass()
my_cmdclass['test'] = PyTest

setup(name='wofs',
      version=versioneer.get_version(),
      cmdclass=my_cmdclass,
      description='Water Observations from Space - Digital Earth Australia',
      long_description=open('README.rst', 'r').read(),
      license='Apache License 2.0',
      url='https://github.com/GeoscienceAustralia/wofs',
      author='Geoscience Australia',
      maintainer='Geoscience Australia',
      maintainer_email='',
      packages=find_packages(),
      data_files=[('wofs/config', config_files)],
      install_requires=[
          'datacube',
      ],
      entry_points={
          'console_scripts': [
              'datacube-wofs = wofs.wofs_app:cli',
          ]
      },
      scripts=['scripts/datacube-wofs-launcher', 'scripts/distributed.sh'])
