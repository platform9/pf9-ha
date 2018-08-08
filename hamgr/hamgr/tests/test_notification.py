import unittest
import logging
from ConfigParser import ConfigParser
from hamgr import notification
from hamgr.notification.notification import Notification

LOG = logging.getLogger(__name__)


@unittest.skip('tests requires rabbitmq server runs locally')
class NotificationPublishTest(unittest.TestCase):

    def setUp(self):
        config = ConfigParser()

        config.set('DEFAULT', 'notification_enabled', 'True')

        config.add_section('amqp')
        config.set('amqp', 'host', 'localhost')
        config.set('amqp', 'port', '5672')
        config.set('amqp', 'username', 'test')
        config.set('amqp', 'password', 'test')
        config.set('amqp', 'exchange_name', 'pf9-changes')
        config.set('amqp', 'exchange_type', 'direct')
        config.set('amqp', 'queue_name', 'pf9-changes-q')

        notification.start(config)

    def tearDown(self):
        notification.stop()

    def test_publish(self):
        notification.publish(Notification('add', 'cluster', '123'))
        LOG.debug("test is done")
