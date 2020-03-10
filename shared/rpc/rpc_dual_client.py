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

import json
import logging

from shared.rpc.rpc_base import RpcBase
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)


class RpcDualClient(RpcBase):
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
                 on_message_callback=None):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._queue_name = queue_name
        self._virtual_host = virtual_host
        self._routing_key = routing_key

        self._consumer_tag = None
        self._callbacks_list = dict()
        self._is_setup_ready = False

        if on_message_callback and callable(on_message_callback):
            self._callbacks_list[str(on_message_callback)] = on_message_callback

        super(RpcDualClient, self).__init__(host,
                                            port,
                                            user,
                                            password,
                                            exchange,
                                            exchange_type,
                                            virtual_host)

    def on_connection_ready(self):
        self._setup_queue(self._queue_name)


    def _setup_queue(self, queue_name):
        LOG.debug('declaring queue %s', queue_name)
        self._channel.queue_declare(self._on_queue_declare_ok, queue_name)

    def _on_queue_declare_ok(self, method_frame):
        LOG.debug('queue declared')
        LOG.debug('binding exchange %s to queue %s with routing %s', self._exchange, self._queue_name, self._routing_key)
        self._channel.queue_bind(self._on_queue_bind_ok, self._queue_name, self._exchange, self._routing_key)

    def _on_queue_bind_ok(self, unused_frame):
        LOG.debug('queue bound')
        LOG.debug('consumer connection is ready, register consumer cancellation callback')
        self.get_channel().add_on_cancel_callback(self._on_consumer_cancelled)
        LOG.debug('consumer connection is ready, register consumer callback to receive messages')
        self._consumer_tag = self.get_channel().basic_consume(self._on_message_received, self._queue_name)
        LOG.debug('producer connection ready, register delivery confirmation callback.')
        self.get_channel().confirm_delivery(self._on_delivery_confirmation)
        self._is_setup_ready = True

    def _on_message_received(self, unused_channel, basic_deliver, properties, body):
        LOG.debug('consumer received message from server, basic_deliver : %s , properties : %s, body : %s',
                  str(basic_deliver),
                  str(properties),
                  str(body))
        tag = basic_deliver.delivery_tag
        LOG.debug('consumer acknowledging received message # %s : %s', tag, body)
        self.get_channel().basic_ack(tag)
        for k, v in self._callbacks_list.iteritems():
            try:
                v({'tag': tag, 'body': json.loads(body)})
            except Exception as e:
                LOG.warning('message is not processed by method %s : %s', str(k), str(e))

    def _on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOG.debug('producer received %s for delivery tag: %i', confirmation_type, method_frame.method.delivery_tag)

    def _on_consumer_cancelled(self, method_frame):
        LOG.debug('consumer was cancelled remotely, shutting down: %r', method_frame)
        if self.get_channel():
            self.get_channel().close()

    def _on_cancel_ok(self, unused_frame):
        LOG.debug('server acknowledged the cancellation of the consumer, now close connection')
        self.close_channel()

    def is_setup_ready(self):
        return self._is_setup_ready

    def on_stopping(self):
        if self.get_channel():
            LOG.debug('consumer is stopping, notifying server for consumer cancellation')
            self.get_channel().basic_cancel(self._on_cancel_ok, self._consumer_tag)
        self._messages_callback = None

    def consume(self, messages_callback):
        if not callable(messages_callback):
            raise Exception('messages_callback needs to be a method')
        LOG.debug('attach callback to receive messages')
        if not self._callbacks_list.has_key(str(messages_callback)):
            self._callbacks_list[str(messages_callback)] = messages_callback

    def publish(self, message, routing=None):
        if self.is_stopping():
            LOG.warning('producer is stopping, so ignore to publish message')
            return

        if not self.is_connected():
            LOG.debug('producer connection is not ready within 30 seconds, ignore current publish request')
            return

        if routing is None:
            routing = self._routing_key

        payload = json.dumps(message, ensure_ascii=False)
        self.get_channel().basic_publish(self._exchange,
                                         routing,
                                         payload)
        LOG.debug('producer published message : %s', payload)
