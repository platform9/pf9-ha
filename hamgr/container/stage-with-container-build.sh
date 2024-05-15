#!/bin/bash

set -xue

cd /buildroot/pf9-ha/hamgr/container && make --max-load=$(nproc) stage-with-py-build-container
