import unittest
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

from ConfigParser import ConfigParser
from hamgr.notification.manager import NotificationManager
from hamgr.notification.model import Notification


@unittest.skip('tests requires rabbitmq server runs locally')
class NotificationPublishTest(unittest.TestCase):

    def setUp(self):
        config = ConfigParser()

        config.set('DEFAULT', 'notification_enabled', 'True')

        config.add_section('amqp')
        config.set('amqp', 'host', 'localhost')
        config.set('amqp', 'port', '5672')
        config.set('amqp', 'username', 'guest')
        config.set('amqp', 'password', 'guest')
        config.add_section('notification')
        config.set('notification', 'exchange_name', 'pf9-changes')
        config.set('notification', 'exchange_type', 'topic')

        self.manager = NotificationManager(config)

    def tearDown(self):
        self.manager = None
        del self.manager

    def test_publish(self):
        self.manager.send_notification(Notification('add', 'cluster', '123'))
        logger.debug("test is done")
