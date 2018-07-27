#!/usr/bin/env python

from setuptools import setup

try:
    import multiprocessing
except ImportError:
    pass

setup(setup_requires=['pbr==3.1.1'], pbr=True)
