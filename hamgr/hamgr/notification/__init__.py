"""
Copyright 2018 Platform9 Systems Inc.(http://www.platform9.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import threading
import logging
from hamgr.notification import publisher
from hamgr.notification import notification as ha_notification
from hamgr.exceptions import ConfigException

LOG = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
_publisher = None
_enabled = False

def start(conf):
    global _publisher
    global _enabled

    # in local debug mode, need to setup log format for pika ioloop, otherwise
    #  no logs from it
    # logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    if not conf:
        raise ConfigException('null conf object')

    # by default assume the notification is not enabled
    _enabled = conf.getboolean("DEFAULT", "notification_enabled") \
        if conf.has_option("DEFAULT", "notification_enabled") else False
    # if not enabled, do nothing
    if not _enabled:
        return

    if not conf and not conf.has_section('amqp'):
        raise ConfigException('invalid config or does not contain section amqp')

    host = conf.get('amqp', 'host')
    port = conf.get('amqp', 'port')
    username = conf.get('amqp', 'username')
    password = conf.get('amqp', 'password')
    exchange = conf.get('amqp', 'exchange_name')
    exchange_type = conf.get('amqp', 'exchange_type')
    queue_name = conf.get('amqp', 'queue_name')
    virtual_host = conf.get('amqp', 'virtual_host') \
        if conf.has_option('amqp', 'virtual_host') else '/'

    error = "empty value for %s in section amqp"
    if not host:
        raise ConfigException(error % 'host')
    if not username:
        raise ConfigException(error % 'username')
    if not password:
        raise ConfigException(error % 'password')
    if not exchange:
        raise ConfigException(error % 'exchange_name')
    if not exchange_type or exchange_type != "direct":
        raise ConfigException(error % 'exchange_type')
    if not queue_name:
        raise ConfigException(error % 'queue_name')
    if not virtual_host:
        raise ConfigException(error % 'virtual_host')

    LOG.info(
        'starting notification publisher, ' \
        'host:%s, port:%s, exchange:%s, exchange key:%s, queue name:%s',
        str(host),
        str(port),
        str(exchange),
        str(exchange_type),
        str(queue_name))
    _publisher = publisher.NotificationPublisher(host=host,
                                                 port=port,
                                                 user=username,
                                                 password=password,
                                                 exchange=exchange,
                                                 exchange_type=exchange_type,
                                                 queue_name=queue_name,
                                                 virtual_host=virtual_host)

    LOG.debug("start notification publisher")
    _publisher.start()
    LOG.debug("ha notification publisher has started")


def stop():
    if not _enabled:
        return

    LOG.debug("stop notification publisher")
    _publisher.stop()


def publish(notification):
    if not _enabled:
        return

    if not notification and not isinstance(notification,
                                           ha_notification.Notification):
        LOG.debug('invalid notification to publish, ignoring it')
        return

    LOG.debug('publishing notification : %s', str(notification))
    key = '.'.join([
        str(notification.action()),
        str(notification.target()),
        str(notification.identifier())
    ])

    if _publisher is None:
        LOG.warn(
            'ha notification publisher is not initialized')
        return

    if not _publisher.is_started():
        LOG.warn(
            'ha notification publisher has not started')
        return

    _publisher.publish(notification, routing=key)
    LOG.debug("published notification %s" % str(notification))
