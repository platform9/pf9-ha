# Copyright 2016 Platform9 Systems Inc.
# All Rights Reserved

import argparse
from glob import glob

from ha.hostapp import manager
from oslo_config import cfg

CONF_DIR = '/etc/vm-ha-helper'
CONF = cfg.CONF


def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Service start script for PF9 service for managing consul')
    parser.add_argument('-d', '--config-dir', action='store',
                        default=CONF_DIR)
    opts = parser.parse_args()

    return opts


def main():
    opts = parse_args()
    # CONF should be setup before any processing starts
    conf_files = glob(opts.config_dir + '/*.conf')
    CONF(default_config_files=conf_files)
    manager.loop()
