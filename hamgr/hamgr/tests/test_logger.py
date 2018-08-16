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
        conf.set("log", "size_bytes", '20')
        conf.set("log", "level", "DEBUG")
        global LOG
        LOG = logger.getLogger(__name__, conf=conf)

    def tearDown(self):
        self._clean()

    def test_logfile_created(self):
        LOG.debug('in testing')
        self.assertTrue(os.path.exists(FILE),
                        'log file %s does not exist' % FILE)

    def test_logfile_rotated(self):
        for i in range(20):
            LOG.debug('This is test log line %d' % i)

        self.assertTrue(os.path.exists(FILE))
        logs = glob.glob("%s*" % FILE)
        self.assertIsNotNone(logs)
        self.assertTrue(len(logs) > 1)
