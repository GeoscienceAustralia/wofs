"""
Setup
-----

"""
import os

from setuptools import setup, find_packages

setup(name='wofs',
      version=os.environ.get('version', 0.0),
      description='Geoscience Australia - WOfS for AGDC',
      long_description=open('README.rst', 'r').read(),
      license='Apache License 2.0',
      url='https://github.com/GeoscienceAustralia/wofs',
      author='Geoscience Australia',
      maintainer='Geoscience Australia',
      maintainer_email='',
      packages=find_packages(),
      install_requires=[
          'datacube',
      ],
      entry_points={
          'console_scripts': [
              'datacube-wofs = wofs.wofs_app:wofs_app',
          ]
      })
