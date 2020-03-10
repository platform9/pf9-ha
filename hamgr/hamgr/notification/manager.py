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
import traceback

from shared.exceptions.ha_exceptions import ConfigException
from shared.rpc.rpc_producer import RpcProducer
from hamgr.notification.model import Notification
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)

_notification_manager = None


class NotificationManager(object):
    _notification_producer = None
    _notification_enabled = False

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(NotificationManager, cls).__new__(cls)
        return cls.instance

    def __init__(self, conf):

        if not conf:
            raise ConfigException('null conf object')

        # by default assume the notification is not enabled
        self._notification_enabled = conf.getboolean("DEFAULT", "notification_enabled") \
            if conf.has_option("DEFAULT", "notification_enabled") else False
        if self._notification_enabled:
            if not conf and not conf.has_section('amqp'):
                raise ConfigException('invalid config or does not contain section amqp')

            if not conf and not conf.has_section('notification'):
                raise ConfigException('invalid config or does not contain section notification')

            host = conf.get('amqp', 'host')
            port = conf.get('amqp', 'port')
            username = conf.get('amqp', 'username')
            password = conf.get('amqp', 'password')
            virtual_host = conf.get('amqp', 'virtual_host') \
                if conf.has_option('amqp', 'virtual_host') else '/'
            error = "empty value for %s in section amqp"
            if not host:
                raise ConfigException(error % 'host')
            if not username:
                raise ConfigException(error % 'username')
            if not password:
                raise ConfigException(error % 'password')
            if not virtual_host:
                raise ConfigException(error % 'virtual_host')

            exchange = conf.get('notification', 'exchange_name')
            exchange_type = conf.get('notification', 'exchange_type')
            routing_key = conf.get('notification', 'routingkey') \
                if conf.has_option('notification', 'routingkey') else ''

            error = "empty value for %s in section 'notification'"
            if not exchange:
                raise ConfigException(error % 'exchange_name')
            if not exchange_type:
                raise ConfigException(error % 'exchange_type')
            msg = 'host:%s, port:%s, exchange:%s, exchange key:%s' % (str(host),
                                                                      str(port),
                                                                      str(exchange),
                                                                      str(exchange_type))
            self._initialized = False
            if self._notification_producer is None:
                LOG.debug('create RPC publisher , %s ', msg)
                self._notification_producer = RpcProducer(host=host,
                                                          port=int(port),
                                                          user=username,
                                                          password=password,
                                                          exchange=exchange,
                                                          exchange_type=exchange_type,
                                                          routing_key=routing_key,
                                                          virtual_host=virtual_host)
                assert self._notification_producer is not None
                LOG.debug("start RPC producer, %s", msg)
                self._notification_producer.start()
                LOG.debug("RPC producer has started, %s", msg)
                self._initialized = True

    def __del__(self):
        if self._notification_enabled:
            LOG.debug("stop notification publisher")
            if self._notification_producer is not None:
                self._notification_producer.stop()

    def send_notification(self, notification):
        if not self._notification_enabled:
            LOG.debug('ignore notification request as notification function is not enabled')
            return

        if not notification or not isinstance(notification, Notification):
            LOG.debug('ignore notification request as the request is invalid notification')
            return

        if self._notification_producer is None:
            LOG.warning('ignore notification request as ha notification publisher is not initialized')
            return

        if not self._notification_producer.is_connected():
            LOG.warning('ignore notification request as ha notification publisher has not started')
            return

        LOG.debug('publishing notification : %s', str(notification))
        self._notification_producer.publish(notification)
        LOG.debug("published notification %s" % str(notification))


def get_notification_manager(config):
    global _notification_manager
    if _notification_manager is None:
        LOG.debug('creating notification manager')
        LOG.debug(traceback.format_exc())
        _notification_manager = NotificationManager(config)

    return _notification_manager
