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
import sys
from shared.constants import ROOT_LOGGER

from six.moves.configparser import ConfigParser


def setup_root_logger(conf=None):
    if conf is None:
        conf = ConfigParser()
        if exists(hamgr.DEFAULT_CONF_FILE):
            with open(hamgr.DEFAULT_CONF_FILE) as fp:
                conf.readfp(fp)

    log_file = hamgr.DEFAULT_LOG_FILE
    if conf.has_option("log", "location"):
        log_file = conf.get("log", "location")

    log_level = hamgr.DEFAULT_LOG_LEVEL
    if conf.has_option("log", "level"):
        log_level = conf.get("log", "level")
    log_mode = 'a'
    log_rotate_count = hamgr.DEFAULT_ROTATE_COUNT
    if conf.has_option("log", "rotate_count"):
        log_rotate_count = conf.get("log", "rotate_count")
    log_rotate_size = hamgr.DEFAULT_ROTATE_SIZE
    if conf.has_option("log", "rotate_size"):
        log_rotate_size = conf.get("log", "rotate_size")
    log_format = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')

    logger = logging.getLogger(ROOT_LOGGER)
    logger.setLevel(log_level)
    
    # to mitigate the drawback of linux built-in log rotation which runs just once a day
    # let the RotatingFileHandler to rotate the log , the built-in log rotation will do
    # daily clean up and archives
    file_handler = logging.handlers.RotatingFileHandler(log_file,
                                                   mode=log_mode,
                                                   maxBytes=int(log_rotate_size),
                                                   backupCount=int(log_rotate_count))
    stderr_handler = logging.StreamHandler(stream=sys.stderr)

    for handler in logger.handlers:                                                         
        logger.removeHandler(handler)

    our_handlers = [stderr_handler, file_handler]
    for handler in our_handlers:
        handler.setLevel(log_level)
        handler.setFormatter(log_format)
        logger.addHandler(handler)
        
    try:
        if dirname(log_file) != '' and not exists(dirname(log_file)):
            makedirs(dirname(log_file))
    except Exception as e:
        logger.exception(e)
        raise
    logger.info('root logger created : name - %s, level - %s, file - %s, '
                 'size - %s, backups - %s', ROOT_LOGGER, str(log_level),
                 log_file, str(log_rotate_size), str(log_rotate_count))
    return logger
