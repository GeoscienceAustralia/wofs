#!/usr/bin/env bash
#/bin/bash
# Script run to make all water extents
# venv is used
if [ $# -lt 1  ]
then
    echo "Usage: $0 /Path2/configfile"
    echo "Usage Example: $0 /g/data/u46/fxz547/wofs2/water_Q12016/client.cfg "
    exit 1
fi

confile=$1

confdir=`dirname $confile`

echo $confile
echo $confdir

module purge 

source activate wofsv1

#additional python modules of project

#TODO: Gradually remove references to this repo's module
NFRIP_ROOT=/g/data/u46/fxz547/Githubz/ga-neo-nfrip
WORKFLOW_DIR=$NFRIP_ROOT/workflow

# New Wofs repo root where all modules will be found, as replacement of the ga-neo-nfrip
WOFSV2_DIR=/g/data/u46/fxz547/Githubz/wofs

# Python Dependencies: agdc api etc
DEPPY=/home/547/fxz547/wofs_run_test/agdc/api/source/main/python:/home/547/fxz547/wofs_run_test/ga-neo-landsat-processor:/home/547/fxz547/wofs_run_test/idl-functions/build/lib.linux-x86_64-2.7

export PYTHONPATH=$WOFSV2_DIR:$NFRIP_ROOT:$NFRIP_ROOT/system/dsm:$NFRIP_ROOT/system/water:$NFRIP_ROOT/system/common:$DEPPY

cd $confdir  # beaware luigi can only find client.cfg in current dir

#python $WORKFLOW_DIR/wofs_summary_v2.py  --config_path ./client.cfg
python $WOFSV2_DIR/wofs/workflow/wofs_make_extents.py  --config_path ./client.cfg

#### Old modules
#python /projects/u46/opt/modules/wofs/1.6.0/ga-neo-nfrip/workflow/wofs_summary.py  --config_path /g/data/u46/wofs/water_Q12016/client.cfg
#python /home/547/fxz547/wofs_run_test/ga-neo-nfrip/workflow/wofs_summary.py  --config_path ./client.cfg

