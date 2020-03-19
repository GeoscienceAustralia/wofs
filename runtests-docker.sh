#!/bin/sh

docker pull opendatacube/datacube-core

# Null out the entrypoint since we aren't (yet) connecting to a database
docker run --entrypoint "" --rm -v $PWD:/tmp/wofs -w /tmp/wofs \
opendatacube/datacube-core:latest /bin/sh -c "pip install dist/*.whl && ./check-code.sh"