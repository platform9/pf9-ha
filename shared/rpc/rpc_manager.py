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
import threading

from shared.exceptions import ha_exceptions
from shared.messages import message_schemas
from shared.messages import message_types
from shared.rpc.rpc_consumer import RpcConsumer
from shared.rpc.rpc_producer import RpcProducer
from shared.constants import LOGGER_PREFIX

from six.moves.queue import Queue

LOG = logging.getLogger(LOGGER_PREFIX + __name__)


class RpcManager(object):
    _rpc_producer = None
    _rpc_consumer = None
    message_buffers = None

    def __init__(self,
                 host,
                 port,
                 username,
                 password,
                 virtual_host,
                 exchange_name,
                 exchange_type,
                 routingkey_for_sending,
                 queue_for_receiving='',
                 routingkey_for_receiving='',
                 application='RpcManager',
                 auto_delete_consumer_queue=False):

        error = "empty value for %s"
        if not host:
            raise ha_exceptions.ArgumentException(error % 'host')
        if not username:
            raise ha_exceptions.ArgumentException(error % 'username')
        if not password:
            raise ha_exceptions.ArgumentException(error % 'password')
        if not virtual_host:
            raise ha_exceptions.ArgumentException(error % 'virtual_host')
        if not exchange_name:
            raise ha_exceptions.ArgumentException(error % 'exchange_name')
        if not exchange_type:
            raise ha_exceptions.ArgumentException(error % 'exchange_type')
        # if not queue_for_receiving:
        #     raise ha_exceptions.ArgumentException(error % 'queue_for_receiving')

        # save the settings
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.routingkey_for_sending = routingkey_for_sending
        self.application = application
        self.queue_for_receiving = queue_for_receiving
        self.routingkey_for_receiving = routingkey_for_receiving
        self.auto_delete_consumer_queue= auto_delete_consumer_queue
        self.enable_consume = False
        if self.queue_for_receiving:
            self.enable_consume = True

        # start both producer and consumer
        self._initialize_rpc()

    def __del__(self):
        if self._rpc_producer:
            self._rpc_producer.stop()
            self._rpc_producer = None
        if self._rpc_consumer:
            self._rpc_consumer.stop()
            self._rpc_consumer = None
        self.message_buffers = None
        self.message_callbacks = None

    def _initialize_rpc(self):
        # close if already initialized
        self.__del__()

        self.message_buffers = dict()
        self.message_callbacks = dict()
        for msg_type in message_schemas.valid_message_types():
            self.message_buffers[msg_type] = Queue()
            self.message_callbacks[msg_type] = []
        # in the rebalance scenario, the controller needs to broadcast request
        # to all hosts
        # the protocol should be like this:
        #  controller -
        #  as request producer, setup exchange as 'direct', use routing key
        #  for requests when publish
        #  as response consumer, use unique queue name, use routing key
        #  for response when consume
        #  hosts -
        #  as request consumer, use unique queue name, use routing key
        #  for requests when consume
        #  as response producer, setup exchange as 'direct', use routng key
        #  for response when publish
        msg = 'host:%s, port:%s, exchange:%s, exchange key:%s' % (
            str(self.host),
            str(self.port),
            str(self.exchange_name),
            str(self.exchange_type))
        if self._rpc_producer is None:
            LOG.debug('create RPC producer for RpcManager, %s, routing '
                      '%s', msg, self.routingkey_for_sending)
            self._rpc_producer = RpcProducer(
                host=self.host,
                port=int(self.port),
                user=self.username,
                password=self.password,
                exchange=self.exchange_name,
                exchange_type=self.exchange_type,
                virtual_host=self.virtual_host,
                routing_key=self.routingkey_for_sending,
                application=self.application
            )
            LOG.debug("start RPC producer for RpcManager, %s, "
                      "routing %s", msg, self.routingkey_for_sending)
            self._rpc_producer.start()
        if self.enable_consume:
            if self._rpc_consumer is None:
                LOG.debug('create RPC consumer for RpcManager, %s, routing '
                          'key %s, queue %s', msg,
                          self.routingkey_for_receiving,
                          self.queue_for_receiving)
                self._rpc_consumer = RpcConsumer(
                    host=self.host,
                    port=int(self.port),
                    user=self.username,
                    password=self.password,
                    exchange=self.exchange_name,
                    exchange_type=self.exchange_type,
                    queue_name=self.queue_for_receiving,
                    virtual_host=self.virtual_host,
                    routing_key=self.routingkey_for_receiving,
                    application=self.application,
                    auto_delete_queue=self.auto_delete_consumer_queue)
                # !!! important : use asynchronous pattern ('push' mode) to
                # receive message rather than 'pulling'
                LOG.debug('set rpc consumer message callback for RpcManager')
                self._rpc_consumer.consume(self._on_message_received)
                LOG.debug('start RPC consumer for RpcManager , %s', msg)
                self._rpc_consumer.start()

    def _on_message_received(self, message):
        LOG.debug('received rpc message : %s', str(message))
        if not message:
            LOG.warning('received rpc message is empty')
            return
        tag = message['tag']
        payload = message['body']
        if payload:
            type = payload['type']
            LOG.debug("received rpc message with payload type %s : %s", type, str(payload))
            if type in message_schemas.valid_message_types():
                self.message_buffers[type].put(message)
                # use thread to unblock message processing
                threading.Thread(name='RpcMsgSubscriber-'+str(type),
                                 target=self._call_subscribers,
                                 args=(type, payload,)).start()
            else:
                LOG.warning('unknown rpc message type '
                            'received %s : %s', type, str(message))

    def _call_subscribers(self, msg_type, msg_payload):
        for msg_callback in self.message_callbacks[msg_type]:
            msg_callback(msg_payload)

    def subscribe_message(self, msg_type, msg_callback):
        if msg_type not in message_schemas.valid_message_types():
            LOG.error('not supported rpc message type %s with callback %s',
                      str(msg_type), str(msg_callback))
            return
        if not msg_callback:
            LOG.warning('callback for rpc message type %s is null or empty',
                        str(msg_type))
            return
        if str(msg_callback) in self.message_callbacks[msg_type]:
            LOG.warning('callback %s has already subscribed to rpc message '
                        'type %s',
                        str(msg_callback), str(msg_type))
            return

        self.message_callbacks[msg_type].append(msg_callback)

    def send_rpc_message(self, message_object, message_type):
        if message_object is None:
            LOG.warning('ignore rpc message as message_object is null or empty')
            return
        if message_type is None:
            LOG.warning('ignore rpc message as message_type is null or empty')
            return
        if not message_schemas.is_allowed_message(message_object, message_type):
            LOG.warning('ignore rpc message as it is not valid as it is '
                        'declared type : %s', str(message_type))
            return

        if self._rpc_producer is None:
            self._initialize_rpc()

        self._rpc_producer.publish(message_object)
        LOG.debug('successfully sent rpc message : %s',
                  str(message_object))
