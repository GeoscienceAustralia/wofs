#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

pylint -j 2 --reports no wofs

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
#py.test -r sx --cov datacube --durations=5 datacube tests $@

