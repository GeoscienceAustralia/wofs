#!/usr/bin/env python

from setuptools import setup, find_packages
from wofs.version import get_version


setup(name='wofs',
      version=get_version(),
      packages=find_packages(
          exclude=('tests', 'tests.*', 'examples',
                   'integration_tests', 'integration_tests.*')
      ),
      package_data={
          '': ['*.cfg'],
          '': ['*.yaml'],
      },
      scripts=[
      ],
      setup_requires=[
          'pytest-runner'
      ],
      install_requires=[
          'click>=6.0',
          'pathlib',
          'pyyaml',
          #'sqlalchemy',
          'python-dateutil',
          'jsonschema',
          'cachetools',
          'numpy',
          'rasterio>=0.28',
          'singledispatch',
          'netcdf4',
          'pypeg2',
          'psycopg2',
          'gdal>=1.9',
          'dask',
          'setuptools',
          'toolz',
          'xarray',
          'scipy',
          'matplotlib',
          'numexpr',
      ],
      tests_require=[
          'pytest',
          'pytest-cov',
          'mock'
      ],
      url='https://github.com/GeoscienceAustralia/wofs',
      author='Fei.Zhang@ga.gov.au',
      maintainer='Fei.Zhang@ga.gov.au',
      maintainer_email='',
      description='Water Observation from Space is software to map water location from satellite remote sensing data',
      long_description=open('README.md', 'r').read(),
      license='Apache License 2.0',
      entry_points={
          'console_scripts': [
              'wofs = wofs.main:cli',
              'wofsclassif = water.classifier:cli',
              'wofssearch = wofs.search:cli'
          ]
      },
      )

