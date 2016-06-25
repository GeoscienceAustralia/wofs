#!/bin/bash

WOFSROOT=/g/data/u46/fxz547/Githubz/wofs
AGDCROOT=/g/data/u46/fxz547/Githubz/agdc-v2

export PYTHONPATH=$WOFSROOT:$AGDCROOT

# separate the two steps
#python wofs/workflow/wofs_setup.py wofs/workflow/wofs_input.yml
#python wofs/workflow/wofs_query.py /g/data/u46/fxz547/wofs2/fxz547_2016-06-13T07-57-50/client.cfg

#cominbined wofs setup and query in 1-click
#python $WOFSROOT/wofs/main.py setup --infile $WOFSROOT/wofs/workflow/wofs_input.yml

# water classification
# (14, -42)
# (14, -41)
# (14, -40)
# (14, -39)
# (15, -42)
# (15, -41)
# (15, -40)
# (15, -39
# (15, -38)
# (16, -42)
# (16, -41)
# (16, -40)
# (16, -39)
# (16, -38)
# (17, -41)
# (17, -40)
# (17, -39)
# (17, -38)
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -42 1991
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -42 1992
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -42 1993
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -42 1994
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -42 1995
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -42 1996
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -42 1997

python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -41 1991
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -41 1992
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -41 1993
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -41 1994
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -41 1995
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -41 1996
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -41 1997

python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -40 1991
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -40 1992
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -40 1993
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -40 1994
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -40 1995
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -40 1996
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -40 1997

python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -39 1991
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -39 1992
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -39 1993
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -39 1994
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -39 1995
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -39 1996
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -39 1997

python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -38 1991
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -38 1992
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -38 1993
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -38 1994
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -38 1995
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -38 1996
python $WOFSROOT/wofs/workflow/make_water_tiles.py 15 -38 1997
