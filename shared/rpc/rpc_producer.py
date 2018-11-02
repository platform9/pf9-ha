# -*- coding: utf-8 -*-
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

import json
import logging

from shared.rpc.rpc_base import RpcBase

LOG = logging.getLogger(__name__)


# ==============================================================================
# module to produce message to RPC exchange/queue
#
# reference source
# https://pika.readthedocs.io/en/latest/examples/asynchronous_publisher_example.html
# https://pika.readthedocs.io/en/latest/examples/asynchronous_consumer_example.html
# ==============================================================================

class RpcProducer(RpcBase):

    def __init__(self,
                 host,
                 port,
                 user,
                 password,
                 exchange,
                 exchange_type,
                 routing_key='',
                 virtual_host="/"):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._virtual_host = virtual_host
        self._routing_key = routing_key

        self._is_setup_ready = False

        super(RpcProducer, self).__init__(host,
                                          port,
                                          user,
                                          password,
                                          exchange,
                                          exchange_type,
                                          virtual_host)

    def _on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOG.debug('producer received %s for delivery tag: %i', confirmation_type, method_frame.method.delivery_tag)

    def on_connection_ready(self):
        LOG.debug('producer connection ready, register delivery confirmation callback.')
        self.get_channel().confirm_delivery(self._on_delivery_confirmation)
        self._is_setup_ready = True

    def on_stopping(self):
        LOG.debug('stopping producer')

    def is_setup_ready(self):
        return self._is_setup_ready

    def publish(self, message, routing=None):
        if self.is_stopping():
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
