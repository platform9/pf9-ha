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
import hamgr
from os.path import exists
from os.path import dirname
from os import makedirs
from shared.constants import ROOT_LOGGER

from six.moves.configparser import ConfigParser


def setup_root_logger(conf=None):
    if conf is None:
        conf = ConfigParser()
        if exists(hamgr.DEFAULT_CONF_FILE):
            with open(hamgr.DEFAULT_CONF_FILE) as fp:
                conf.readfp(fp)

    log_level = hamgr.DEFAULT_LOG_LEVEL
    if conf.has_option("log", "level"):
        log_level = conf.get("log", "level")
    
    log_format = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')

    logger = logging.getLogger(ROOT_LOGGER)
    logger.setLevel(log_level)

    # Clear existing handlers
    for hdl in logger.handlers:
        logger.removeHandler(hdl)
    
    # Only add a StreamHandler to write logs to stdout
    stdout_handler = logging.StreamHandler()
    stdout_handler.setLevel(log_level)
    stdout_handler.setFormatter(log_format)
    logger.addHandler(stdout_handler)

    logger.info('root logger created : name - %s, level - %s, stdout only', 
                 ROOT_LOGGER, str(log_level))
    return logger
