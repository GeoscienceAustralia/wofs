#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

shopt -s globstar

pylint -j 2 --ignore-patterns='.+\.so' --reports no wofs

# We check the integration tests even though they aren't run by default here.
# pylint --rcfile=integration_tests/pylintrc integration_tests/**/*.py

# E122: 'continuation line' has too many spurious errors.
# E711: "is None" instead of "= None". Duplicates pylint check.
# E701: "multiple statements on one line" is buggy as it doesn't understand py 3 types
# E501: "line too long" duplicates pylint check
pycodestyle --ignore=E122,E711,E701,E501 --max-line-length 120  \
    wofs \
    wofs-summary/*.py \

shellcheck ./**/*.sh
yamllint ./**/*.yaml

# Users can specify extra folders (ie. integration_tests) as arguments.
#py.test -r sx --cov wofs --durations=5 wofs scripts/**/*.py "$@"