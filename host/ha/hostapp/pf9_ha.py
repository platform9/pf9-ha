# Copyright (c) 2019 Platform9 Systems Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
from glob import glob

from ha.hostapp import manager
from oslo_config import cfg

from ha.utils.log import setup_root_logger

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
    # root logger setup should be done in main entry point
    # before using any logging method
    setup_root_logger()
    manager.loop()
