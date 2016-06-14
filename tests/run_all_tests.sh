#!/bin/bash

WOFSROOT=/g/data/u46/fxz547/Githubz/wofs
AGDCROOT=/g/data/u46/fxz547/Githubz/agdc-v2

export PYTHONPATH=$WOFSROOT:$AGDCROOT

# separate the two steps
#python wofs/workflow/wofs_setup.py wofs/workflow/wofs_input.yml
#python wofs/workflow/wofs_query.py /g/data/u46/fxz547/wofs2/fxz547_2016-06-13T07-57-50/client.cfg

#cominbined wofs setup and query in 1-click
python $WOFSROOT/wofs/main.py setup --infile $WOFSROOT/wofs/workflow/wofs_input.yml

# water classification
python $WOFSROOT/wofs/workflow/make_water_tiles.py
