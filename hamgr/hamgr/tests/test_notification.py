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
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

from hamgr.notification import NotificationManager
from shared.messages.cluster_event import ClusterEvent

from six.moves.configparser import ConfigParser


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
        self.manager.send_notification(ClusterEvent('add', 'cluster', '123'))
        logger.debug("test is done")
