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
from oslo_config import cfg
from shared.constants import ROOT_LOGGER

CONF = cfg.CONF
log_group = cfg.OptGroup('log', title='Group for all log options')
log_opts = [
    cfg.StrOpt('level', default='DEBUG', help='Log level'),
    cfg.StrOpt('file', default='/var/log/pf9/pf9-ha.log',
               help='log file location'),
    cfg.StrOpt('max_bytes', default='10485760', help='max size in bytes of log file when to rotate'),
    cfg.StrOpt('backup_count', default='5', help='num of log files to rotate')
]
CONF.register_group(log_group)
CONF.register_opts(log_opts, log_group)
LOG_FILE = CONF.log.file
LOG_LEVEL = CONF.log.level
LOG_MAX_BYTES = int(CONF.log.max_bytes)
LOG_BACKUP_COUNT = int(CONF.log.backup_count)


def setup_root_logger():
    logger = logging.getLogger(ROOT_LOGGER)
    logger.setLevel(LOG_LEVEL)
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    handler = logging.handlers.RotatingFileHandler(LOG_FILE,
                                                   mode='a',
                                                   maxBytes=LOG_MAX_BYTES,
                                                   backupCount=LOG_BACKUP_COUNT)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    for hdl in logger.handlers:
        logger.removeHandler(hdl)
    logger.addHandler(handler)
    logger.info('root logger created : name - %s , level - %s', ROOT_LOGGER, LOG_LEVEL)
    return logger