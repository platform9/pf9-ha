# -*- coding: utf-8 -*-
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
import json
from datetime import datetime
from shared.rpc.rpc_channel import RpcChannel
from shared.constants import LOGGER_PREFIX

from six import iteritems

LOG = logging.getLogger(LOGGER_PREFIX + __name__)


# ==============================================================================
# module to consume messages from RPC queue
#
# reference source
# https://pika.readthedocs.io/en/latest/examples/asynchronous_consumer_example.html
# ==============================================================================

class RpcConsumer(object):
    def __init__(self,
                 host,
                 port,
                 user,
                 password,
                 exchange,
                 exchange_type,
                 queue_name,
                 routing_key='',
                 virtual_host="/",
                 application='',
                 auto_delete_queue=False):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._queue_name = queue_name
        self._virtual_host = virtual_host
        self._application = application
        self._routing_key = routing_key
        self._auto_delete_queue = auto_delete_queue

        self._rpc_channel = RpcChannel(host,
                                       port,
                                       user,
                                       password,
                                       exchange,
                                       exchange_type,
                                       virtual_host,
                                       application=application)
        self._consumer_tag = None
        self._callbacks_list = dict()

    def start(self):
        LOG.debug('starting RPC consumer for %s', self._application)
        self._rpc_channel.start()
        # register callbacks
        # - connection_ready
        # - connection_close
        self._rpc_channel.add_connection_ready_callback(
            self.on_connection_ready)
        self._rpc_channel.add_connection_close_callback(
            self.on_connection_close)
        LOG.info('started RPC consumer for %s', self._application)

    def stop(self):
        if self._rpc_channel:
            self._rpc_channel.stop()
            self._rpc_channel = None

    def _on_message_received(self, unused_channel, basic_deliver, properties,
                             body):
        LOG.debug('consumer received message from server at %s: %s',
                  str(datetime.utcnow()), str(body))
        tag = basic_deliver.delivery_tag
        LOG.debug('consumer acknowledging received message # %s : %s', tag,
                  body)
        channel = self._rpc_channel.get_channel()
        channel.basic_ack(tag)
        for k, v in iteritems(self._callbacks_list):
            try:
                v({'tag': tag, 'body': json.loads(body)})
            except Exception as e:
                LOG.warning('message is not processed by method %s : %s',
                            str(k), str(e))

    def _on_consumer_cancelled(self, method_frame):
        LOG.debug('consumer was cancelled remotely, shutting down: %r',
                  method_frame)
        channel = self._rpc_channel.get_channel()
        if channel:
            channel.close()

    def _on_cancel_ok(self, unused_frame):
        LOG.debug("server acknowledged the cancellation of the consumer, "
                  "now close connection")
        self._rpc_channel.close_channel()

    def on_connection_close(self):
        channel = self._rpc_channel.get_channel()
        if channel:
            LOG.debug('RPC consumer is stopping, notifying server for '
                      'consumer cancellation')
            channel.basic_cancel(self._on_cancel_ok, self._consumer_tag)
        self._messages_callback = None

    def _setup_queue(self, queue_name):
        LOG.debug('RPC consumer declaring queue %s', queue_name)
        try:
            channel = self._rpc_channel.get_channel()
            channel.queue_declare(self._on_queue_declare_ok, queue_name,
                                        auto_delete=self._auto_delete_queue)
        except Exception:
            LOG.exception('unhandled RPC consumer queue %s declaration',
                          queue_name)

    def _on_queue_declare_ok(self, method_frame):
        LOG.debug(
            'RPC consumer binding exchange %s to queue %s with routing %s',
            self._exchange, self._queue_name, self._routing_key)
        channel = self._rpc_channel.get_channel()
        channel.queue_bind(self._on_queue_bind_ok, self._queue_name,
                           self._exchange, self._routing_key)

    def _on_queue_bind_ok(self, unused_frame):
        LOG.debug('RPC consumer queue is ready')
        channel = self._rpc_channel.get_channel()
        channel.add_on_cancel_callback(self._on_consumer_cancelled)
        LOG.debug('register RPC consumer callback to receive messages')
        self._consumer_tag = channel.basic_consume(
            self._on_message_received, self._queue_name)

    def on_connection_ready(self):
        LOG.debug('RPC consumer connection is ready, now setup queue')
        self._setup_queue(self._queue_name)

    def consume(self, messages_callback):
        if not callable(messages_callback):
            raise Exception('messages_callback needs to be a method')
        LOG.debug('attach callback to receive RPC messages')
        if str(messages_callback) not in self._callbacks_list:
            self._callbacks_list[str(messages_callback)] = messages_callback
