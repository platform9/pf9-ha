#!/usr/bin/env sh

set -e
# the cryptography dependency requires higher version of pip and setuptools
# during tox install cryptography package, so customize tox virtualenv
# before install the dependencies
echo 'upgrade pip ...'
pip install -U pip
echo 'upgrade setuptools ...'
pip install -U setuptools
echo 'install dependencies from ' + $@
pip install $@