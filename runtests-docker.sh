#!/bin/sh

#docker pull opendatacube/datacube-core

docker run --entrypoint "" --rm -v $PWD:/tmp/wofs -w /tmp/wofs opendatacube/datacube-core:latest /bin/sh -c "python3 setup.py sdist bdist_wheel && pip install -e .[terrain] && ./check-code.sh"