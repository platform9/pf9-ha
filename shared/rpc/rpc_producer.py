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

import json
import logging
from datetime import datetime, timedelta
import time
import threading
from shared.rpc.rpc_channel import RpcChannel
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)


# ==============================================================================
# module to produce message to RPC exchange/queue
#
# reference source
# https://pika.readthedocs.io/en/latest/examples/asynchronous_publisher_example.html
# https://pika.readthedocs.io/en/latest/examples/asynchronous_consumer_example.html
# ==============================================================================

class RpcProducer(object):

    def __init__(self,
                 host,
                 port,
                 user,
                 password,
                 exchange,
                 exchange_type,
                 routing_key='',
                 virtual_host="/",
                 application=''):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._virtual_host = virtual_host
        self._routing_key = routing_key
        self._application = application

        self._rpc_channel = RpcChannel(host,
                                       port,
                                       user,
                                       password,
                                       exchange,
                                       exchange_type,
                                       virtual_host,
                                       application = application
                                       )
        self._sending_buffer = []
        self._sending_stop = False
        self._sending_thread = None
        self._sending_interval_secs = 10

    def start(self):
        LOG.debug('starting RPC producer for %s', self._application)
        self._rpc_channel.start()
        # setup callbacks
        # - connection_ready
        # - connection_close
        self._sending_stop = False
        self._rpc_channel.add_connection_ready_callback(
            self.on_connection_ready)
        self._rpc_channel.add_connection_close_callback(
            self.on_connection_close)
        LOG.info('RPC producer for %s started', self._application)
        self._sending_thread = threading.Thread(name='RpcProducerMsgSending',
                                                target=self._sending_messages)
        self._sending_thread.start()

    def stop(self):
        LOG.debug('stopping RPC producer for %s', self._application)
        self._sending_stop = True
        if self._rpc_channel:
            self._rpc_channel.stop()
            self._rpc_channel = None
        if self._sending_thread:
            self._sending_thread.join(5)
            self._sending_thread = None
        LOG.debug('RPC producer for %s stopped', self._application)

    def _on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOG.debug('producer for %s received %s for delivery tag: %i',
                  self._application, confirmation_type, method_frame.method.delivery_tag)

    def on_connection_ready(self):
        LOG.debug('producer connection ready for %s, register delivery confirmation '
                  'callback.', self._application)
        channel = self._rpc_channel.get_channel()
        channel.confirm_delivery(self._on_delivery_confirmation)
        self._is_setup_ready = True

    def on_connection_close(self):
        LOG.debug('stopping RPC producer for %s', self._application)
        self._sending_stop = True

    def publish(self, message, routing=None):
        # to support asynchronous operation, buffer the message for sending
        self._sending_buffer.append({
            'message': message,
            'routing': routing
        })

    def _sending_messages(self):
        """
        the main thread to drain messages in buffer and send to rabbitmq
        :return:
        """
        LOG.debug('RPC producer sending thread for %s started', self._application)
        while not self._sending_stop:
            payload = None
            try:
                # remove any staled message
                shadow_buffer = self._sending_buffer[:]
                for msg in shadow_buffer:
                    timestamp = datetime.strptime(msg['message']['timestamp'],
                                                  '%Y-%m-%d %H:%M:%S')
                    if (datetime.utcnow() - timestamp) > timedelta(minutes=5):
                        self._sending_buffer.remove(msg)

                # drain all messages in buffer
                size = len(self._sending_buffer)
                if not size:
                    time.sleep(self._sending_interval_secs)
                    continue
                all_msgs = str(self._sending_buffer)
                LOG.debug('size of PRC producer sending buffer for %s : %s , messages : %s', self._application, str(size), all_msgs)
                connection = self._rpc_channel.get_connection()
                if not connection:
                    LOG.warning('RPC producer for %s can not publish message because '
                                'rpc connection null', self._application)
                    time.sleep(self._sending_interval_secs)
                    continue
                if connection.is_closed:
                    LOG.warning('RPC producer for %s can not publish message because '
                                'rpc connection is closed', self._application)
                    self._rpc_channel.restart()
                    time.sleep(self._sending_interval_secs)
                    continue
                channel = self._rpc_channel.get_channel()
                if not channel:
                    LOG.warning('RPC producer for %s can not publish message because '
                                'rpc channel is null', self._application)
                    time.sleep(self._sending_interval_secs)
                    continue
                if channel.is_closed:
                    LOG.warning('RPC producer for %s can not publish message because '
                                'rpc channel is closed', self._application)
                    self._rpc_channel.restart()
                    time.sleep(self._sending_interval_secs)
                    continue
                # because message append to the end of queue
                # so here drain from the front [0...size)
                for i in range(0, size):
                    # always pop at 0
                    item = self._sending_buffer.pop(0)
                    if not item:
                        continue
                    message = item['message']
                    routing = item['routing']
                    if routing is None:
                        routing = self._routing_key

                    payload = json.dumps(message, ensure_ascii=False)
                    LOG.debug('RPC producer for %s publish message at %s : %s',
                              self._application, str(datetime.utcnow()), payload)
                    channel.basic_publish(self._exchange,
                                          routing,
                                          payload)
                    LOG.debug('RPC producer for %s published message (%s:%s) at %s : %s',
                              self._application, str(datetime.utcnow()),
                              self._exchange, routing, payload)
            except Exception:
                LOG.exception('exception when RPC producer for %s sending message : %s',
                              self._application, str(payload))
            # don't spin CPU too fast
            time.sleep(self._sending_interval_secs)
        LOG.debug('RPC producer sending thread for %s stopped', self._application)
