#!/usr/bin/env python3

import codecs
import os
import re

from setuptools import find_packages
from distutils.core import setup
from distutils.command.sdist import sdist

here = os.path.abspath(os.path.dirname(__file__))
config_files = ['config/' + name for name in os.listdir('config')]
tests_require = ['pytest', 'pytest-cov', 'mock', 'pycodestyle', 'pylint',
                 'hypothesis', 'compliance-checker', 'yamllint']
extras_require = {
    'doc': ['Sphinx', 'nbsphinx', 'setuptools', 'sphinx_rtd_theme', 'IPython', 'jupyter_sphinx',
            'recommonmark'],
    'test': tests_require,
}


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


setup(
    name='wofs',
    version=find_version("wofs", "__init__.py"),
    cmdclass={'sdist': sdist},
    url='https://github.com/GeoscienceAustralia/wofs',
    description='Water Observations from Space - Digital Earth Australia',
    long_description=open('README.rst', 'rt').read(),
    author='Geoscience Australia',
    author_email='damien.ayers@ga.gov.au',
    maintainer='Geoscience Australia',
    maintainer_email='',
    license='Apache License 2.0',
    packages=find_packages(),
    package_data={
        '': ['*.yaml', '*/*.yaml'],
    },
    include_package_data=True,
    scripts=['scripts/datacube-wofs-launcher', 'scripts/distributed.sh'],
    setup_requires=[
        'pytest-runner'
    ],
    data_files=[('wofs/config', config_files)],
    install_requires=[
          'datacube',
    ],
    tests_require=tests_require,
    extras_require=extras_require,
    entry_points={
        'console_scripts': [
              'datacube-wofs = wofs.wofs_app:cli',
        ]
    },
)
