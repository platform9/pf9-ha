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

import logging
import logging.config
import logging.handlers

from os import makedirs
from os.path import dirname
from os.path import exists

from oslo_config import cfg

CONF = cfg.CONF
log_group = cfg.OptGroup('log', title='Group for all log options')
log_opts = [
    cfg.StrOpt('level', default='INFO', help='Log level'),
    cfg.StrOpt('file', default='/var/log/pf9/pf9-ha.log',
               help='log file location')
]
CONF.register_group(log_group)
CONF.register_opts(log_opts, log_group)
LOG_FILE = CONF.log.file


def setup_log_dir_and_file(filename, mode='a', encoding=None, owner=None):
    try:
        if not exists(dirname(filename)):
            makedirs(dirname(filename))
        if not exists(filename):
            open(filename, mode).close()
        return logging.FileHandler(filename, mode=mode)
    except Exception:
        return logging.NullHandler()


def setup_logger():
    log_config = {
        'version': 1.0,
        'formatters': {
            'default': {
                'format': '%(asctime)s %(name)s %(levelname)s %(message)s'
            },
        },
        'handlers': {
            'file': {
                '()': setup_log_dir_and_file,
                'level': CONF.log.level,
                'formatter': 'default',
                'filename': LOG_FILE,
            },
        },
        'root': {
            'handlers': ['file'],
            'level': CONF.log.level,
        },
    }
    logging.config.dictConfig(log_config)


def getLogger(logger_name):
    return logging.getLogger(logger_name)

setup_logger()
