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
import ConfigParser
import hamgr
from os.path import exists
from os.path import dirname
from os import makedirs


def getLogger(name, conf=None):
    if conf is None:
        conf = ConfigParser.ConfigParser()
        if exists(hamgr.DEFAULT_CONF_FILE):
            with open(hamgr.DEFAULT_CONF_FILE) as fp:
                conf.readfp(fp)

    log_file = conf.get("log", "location") if conf.has_option("log", "location") else hamgr.DEFAULT_LOG_FILE
    log_level = conf.get("log", "level") if conf.has_option("log", "level") else hamgr.DEFAULT_LOG_LEVEL
    log_mode = 'a'

    # the basic config for logging
    log_format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    logging.basicConfig(filename=log_file, level=log_level, format=log_format)
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    try:
        if dirname(log_file) != '' and not exists(dirname(log_file)):
            makedirs(dirname(log_file))
    except Exception as e:
        logger.exception(e)
        raise
    return logger
