import os

from setuptools import find_packages, setup

config_files = ['config/' + name for name in os.listdir('config')]
tests_require = ['pytest', 'pytest-cov', 'mock', 'pycodestyle', 'pylint',
                 'hypothesis', 'compliance-checker', 'yamllint']
extras_require = {
    'terrain': ['scipy', 'ephem'],
    'test': tests_require,
}

setup(
    name='wofs',
    description='Water Observations from Space - Digital Earth Australia',
    long_description=open('README.rst', 'r').read(),
    license='Apache License 2.0',
    url='https://github.com/GeoscienceAustralia/wofs',
    author='Geoscience Australia',
    maintainer='Geoscience Australia',
    maintainer_email='',
    packages=find_packages(),
    data_files=[('wofs/config', config_files)],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.6",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering :: GIS",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
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
