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
import ConfigParser
from hamgr import logger

LOG = None
FILE = "log.txt"


class LogConfigTest(unittest.TestCase):
    def _clean(self):
        if os.path.exists(FILE):
            logs = glob.glob("%s*" % FILE)
            for log in logs:
                os.remove(log)

    def setUp(self):
        self._clean()
        conf = ConfigParser.ConfigParser()
        conf.add_section("log")
        conf.set("log", "location", FILE)
        conf.set("log", "rotate_counts", '5')
        conf.set("log", "rotate_size", '1024')
        conf.set("log", "level", "DEBUG")
        global LOG
        LOG = logger.setup_root_logger(conf=conf)

    def tearDown(self):
        self._clean()
        pass

    def test_logfile_created(self):
        LOG.debug('in testing')
        self.assertTrue(os.path.exists(FILE),
                        'log file %s does not exist' % FILE)

    def test_logfile_rotated(self):
        for i in range(20000):
            LOG.debug('This is test log line %d' % i)

        self.assertTrue(os.path.exists(FILE))
        logs = glob.glob("%s*" % FILE)
        self.assertIsNotNone(logs)
        self.assertTrue(len(logs) >= 1)
        for log in logs:
            size=os.path.getsize(log)
            LOG.debug('size of file %s : %s', log, str(size))
            self.assertTrue(size <= 1024)
