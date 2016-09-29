Normalised Difference Vegetation Index (ndvi)
=============================================

Live green plants absorb solar radiation in the photosynthetically 
active radiation (PAR) spectral region, which they use as a source 
of energy in the process of photosynthesis. Leaf cells have also 
evolved to reflect, scatter and re-emit solar radiation in the 
near-infrared spectral region.
 
The pigment in plant leaves, chlorophyll, strongly absorbs visible 
light (from 0.4 to 0.7 micrometres) for use in photosynthesis. 
The cell structure of the leaves, on the other hand, strongly 
reflects near-infrared light (from 0.7 to 1.1 micrometres). 
Hence, live green plants appear relatively dark in the PAR and 
relatively bright in the near-infrared. 

By contrast, clouds and snow tend to be rather bright in the red 
(as well as other visible wavelengths) and quite dark in the 
near-infrared.  


Installation
------------
To install the module on raijin:

Update Collection Management Interface system
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Go to http://52.62.11.43/validate/11

Ensure that the global attributes from CMI match the global attributes
in the config files, and update appropriately.

Download from GitHub
~~~~~~~~~~~~~~~~~~~~

Checkout the tagged branch you wish to install to temp directory::

    git clone git@github.com:GeoscienceAustralia/ndvi.git
    cd ndvi
    git checkout tags/1.0.2
    git describe --tags --always

The tagged version should be printed.

Then to install::

    $ module use /g/data/v10/public/modules/modulefiles/
    $ sh package-module.sh 

You will be promted to check the package location and version.
If it is correct, type **y** and press enter
::

    # Packaging agdc-ndvi v 1.0.0 to /g/data/v10/public/modules/agdc-ndvi/1.0.0 #
    Continue?

Setup on VDI

The first time you try to use raijin PBS commands from VDI, you will need
to run::

    $ remote-hpc-cmd init

See http://vdi.nci.org.au/help#heading=h.u1kl1j7vdt16 for more details.

You will also need to setup datacube to work from VDI and rajin.
::

    $ ssh raijin "cat .pgpass" >> ~/.pgpass
    $ chmod 0600 ~/.pgpass

See http://agdc-v2.readthedocs.io/en/stable/user/nci\_usage.html for
full details.

Running
-------

The NDVI application works in 2 parts:

#. Creating the task list
    * Checking the database connection
    * Quering the database for potential input datasets (NBAR)
    * Quering the database for existing output datasets (previously generated NDVI)
    * Check for unexpected existing files - these were most likely created during an run that did not successfully finish.
#. Submit the job to raijin

To run ndvi::

    $ module use /g/data/v10/public/modules/modulefiles/
    $ module load agdc-ndvi
    $ datacube-ndvi-launcher list

This will list the availiable app configs::

    ls5_ndvi_albers.yaml
    ls7_ndvi_albers.yaml
    ls8_ndvi_albers.yaml

To submit the job to `raijin`, the launcher has a the ``qsub`` command:

Usage: ``datacube-ndvi-launcher qsub [OPTIONS] APP_CONFIG YEAR``

Options:

* ``-q, --queue normal``            The queue to use, either ``normal`` or ``express``
* ``-P, --project v10``             The project to use
* ``-n, --nodes INTEGER RANGE``     Number of *nodes* to request  [required]
* ``-t, --walltime 4``              Number of *hours* to request
* ``--name TEXT``                   Job name to use
* ``--config PATH``                 Datacube config file (be default uses the currently loaded AGDC module)
* ``--env PATH``                    Node environment setup script (by default uses the installed production environment)
* ``--help``                        Show help message.

Change your working directory to a location that can hold the task file, 
and run the launcher specifying the app config, year (``1993`` or a range ``1993-1996``), and PBS properties:
::

    $ cd /g/data/v10/tmp
    $ datacube-ndvi-launcher qsub ls5_ndvi_albers.yaml 1993-1996 -q normal -P v10 -n 1 --walltime 10

We have found for best throughput *1 node* can produce about 260 tiles per minute per node, with a CPU efficiency of about 80%.

It will check to make sure it can access the database::

    Version: 1.1.9
    Read configurations files from: ['/g/data/v10/public/modules/agdc-py2-prod/1.1.9/datacube.conf']
    Host: 130.56.244.227:6432
    Database: datacube
    User: adh547

    Attempting connect
    Success.

Then it will create the task file in the current working directory, and create the output product
definition in the database (if it doesn't already exist)::

    datacube-ndvi -v --app-config "/g/data/v10/public/modules/agdc-ndvi/1.0.1/config/ls5_ndvi_albers.yaml" --year 1993-1996 --save-tasks "/g/data/v10/tmp/ls5_ndvi_albers_1993-1996.bin"
    RUN? [Y/n]:

    2016-07-13 18:38:56,308 INFO Created DatasetType ls5_ndvi_albers
    2016-07-13 18:39:01,997 INFO 291 tasks discovered
    2016-07-13 18:39:01,998 INFO 291 tasks discovered
    2016-07-13 18:39:02,127 INFO Saved config and tasks to /g/data/v10/tmp/ls5_ndvi_albers_1993-1996.bin

It will loop through every task::

    datacube-ndvi -v --load-tasks "/g/data/v10/tmp/ls5_ndvi_albers_test_1993-1996.bin" --dry-run
    RUN? [y/N]:

    Starting NDVI processing...
    Files to be created:
    /g/data/fk4/datacube/002/LS5_TM_NDVI/15_-39/LS5_TM_NDVI_3577_15_-39_19930513231246500000.nc
    /g/data/fk4/datacube/002/LS5_TM_NDVI/15_-40/LS5_TM_NDVI_3577_15_-40_19930513231246500000.nc
    ...
    291 tasks files to be created (291 valid files, 0 existing paths)
    
If any output files already exist, you will be asked if they should be deleted.

Then it will ask to confirm the job should be submitted to PBS::

    qsub -q normal -N ls5_ndvi_albers_1993-1996 -P v10 -l ncpus=16,mem=31gb,walltime=1:00:00,other=gdata2 -- /bin/bash "/g/data/v10/public/modules/agdc-ndvi/1.0.1/scripts/distributed.sh" --ppn 16 datacube-ndvi -v --load-tasks "/g/data/v10/tmp/ls5_ndvi_albers_1993-1996.bin" --executor distributed DSCHEDULER
    RUN? [Y/n]:

It should then return a job id, such as `7517348.r-man2`

If you say `no` to the last step, the task file you created can be submitted to qsub later by calling::

    datacube-ndvi-launcher qsub -q normal -P v10 -n 1 --taskfile "/g/data/v10/tmp/ls5_ndvi_albers_1993-1996.bin" ls5_ndvi_albers.yaml


Tracking progress
-----------------

::

    $ qstat -u $USER

    $ qcat 7517348.r-man2 | head

    $ qcat 7517348.r-man2 | tail

    $ qps 7517348.r-man2

(TODO: Add instructions to connect to ``distributed`` web interface...)

File locations
--------------

The config file (eg. ls5_ndvi_albers.yaml) specifies the app settings, and is found in the module.

You will need to check the folder of the latest ``agdc-ndvi`` module::

    ls /g/data/v10/public/modules/agdc-ndvi/

To view the app config file, replace ``1.0.0`` with the latest version from above. 
::

    head /g/data/v10/public/modules/agdc-ndvi/1.0.0/config/ls5_ndvi_albers_test.yaml
    
The config file lists the output `location` and file_path_template``, as shown in this snippet::

    source_type: ls5_nbar_albers
    output_type: ls5_ndvi_albers
    version: 1.0.0
    
    description: Landsat 5 Normalised Difference Vegetation Index 25 metre, 100km tile, Australian Albers Equal Area projection (EPSG:3577)
    product_type: ndvi
    
    location: '/g/data/fk4/datacube/002/'
    file_path_template: 'LS5_TM_NDVI/{tile_index[0]}_{tile_index[1]}/LS5_TM_NDVI_3577_{tile_index[0]}_{tile_index[1]}_{start_time}.nc'

So here the output files are saved to ``/g/data/fk4/datacube/002/LS5_TM_NDVI/<tile_index>/*.nc``
