#!/usr/bin/env python

from setuptools import setup

try:
    import multiprocessing
except ImportError:
    pass

setup(setup_requires=['pbr==1.8.1'], pbr=True)
