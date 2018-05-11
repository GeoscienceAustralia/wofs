"""
Setup
-----

"""

import codecs
import os
import re

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
config_files = ['config/' + name for name in os.listdir('config')]


def read(*parts):
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(name='wofs',
      version=find_version("wofs", "__init__.py"),
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
              'datacube-wofs = wofs.wofs_app:wofs_app',
          ]
      },
      scripts=['scripts/datacube-wofs-launcher', 'scripts/distributed.sh'])
