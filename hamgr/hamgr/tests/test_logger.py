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

import unittest
import glob
import os
from hamgr import logger

from six.moves.configparser import ConfigParser

LOG = None
FILE = "log.txt"


class LogConfigTest(unittest.TestCase):
    def setUp(self):
        conf = ConfigParser()
        conf.add_section("log")
        conf.set("log", "level", "DEBUG")
        global LOG
        LOG = logger.setup_root_logger(conf=conf)

    def tearDown(self):
        pass

    def test_stdout_handler_added(self):
        # Verify that a StreamHandler is added to the logger
        handlers = LOG.handlers
        stream_handlers = [h for h in handlers if isinstance(h, logger.logging.StreamHandler)]
        self.assertTrue(len(stream_handlers) > 0, 'No StreamHandler found in logger handlers')
        
    def test_no_file_handlers(self):
        # Verify that there are no file handlers
        handlers = LOG.handlers
        file_handlers = [h for h in handlers if isinstance(h, logger.logging.handlers.RotatingFileHandler)]
        self.assertEqual(len(file_handlers), 0, 'Found file handlers when only stdout should be used')
