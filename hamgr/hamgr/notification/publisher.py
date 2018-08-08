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
import datetime
import sys
import logging
import threading
import pika
import json
import Queue

LOG = logging.getLogger(__name__)


# ==============================================================================
# module to publish notification asynchronously to configured queue
#
# reference source
# https://pika.readthedocs.io/en/latest/examples/asynchronous_publisher_example.html
# ==============================================================================


class NotificationPublisher(object):

    def __init__(self,
                 host,
                 port,
                 user,
                 password,
                 exchange,
                 exchange_type,
                 queue_name,
                 virtual_host="/"):
        # store parameters in local
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._virtual_host = virtual_host
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._queue_name = queue_name

        self._connection = None
        self._channel = None
        self._stopping = False
        self._messages = Queue.Queue()
        self._lock = threading.Lock()
        self._thread_ioloop = None
        self._thread_publish = None
        self._started = False

    def connect(self):
        LOG.debug('connecting to %s:%s', self._host, self._port)
        credentials = pika.PlainCredentials(username=self._user,
                                            password=self._password)
        parameters = pika.ConnectionParameters(host=self._host,
                                               port=self._port,
                                               virtual_host=self._virtual_host,
                                               credentials=credentials)
        return pika.SelectConnection(parameters=parameters,
                                     on_open_callback=self.on_connection_open,
                                     on_open_error_callback=self.on_connection_open_error,
                                     on_close_callback=self.on_connection_closed)

    def on_connection_open(self, unused_connection):
        LOG.debug('opening connection, try open channel')
        self.open_channel()

    def on_connection_open_error(self, unused_connection, err):
        LOG.debug('opening connection failed %s , try to stop ioloop', err)
        # self._connection.ioloop.call_later(5, self._connection.ioloop.stop)
        self._connection.ioloop.stop()

    def on_connection_closed(self, connection, reason, code):
        LOG.debug('connection closed , %s , %s ', reason,
                  code)
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            LOG.debug('connection closed, try to stop ioloop')
            self._connection.ioloop.stop()

    def open_channel(self):
        LOG.debug('opening channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOG.debug('channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self._exchange)

    def add_on_channel_close_callback(self):
        LOG.debug('adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason, code):
        LOG.debug('channel was closed , %s,  %s', reason, code)
        self._channel = None
        if not self._stopping:
            LOG.debug('channel was closed, try close connection')
            self._connection.close()

    def setup_exchange(self, exchange_name):
        LOG.debug('declaring exchange %s', exchange_name)
        self._channel.exchange_declare(exchange=exchange_name,
                                       exchange_type=self._exchange_type,
                                       callback=self.on_exchange_declareok,
                                       passive=False,
                                       durable=False,
                                       auto_delete=False)

    def on_exchange_declareok(self, unused_frame):
        LOG.debug('exchange declared, setup queue %s', self._queue_name)
        self.setup_queue(self._queue_name)

    def setup_queue(self, queue_name):
        LOG.debug('declaring queue name %s', queue_name)
        self._channel.queue_declare(queue=queue_name,
                                    callback=self.on_queue_declare_ok,
                                    passive=False,
                                    durable=False,
                                    auto_delete=False)

    def on_queue_declare_ok(self, method_frame):
        LOG.debug('queue declared, bind exchange %s to queue %s ',
                  self._exchange, self._queue_name)
        self._channel.queue_bind(queue=self._queue_name,
                                 exchange=self._exchange,
                                 callback=self.on_queue_bind_ok)

    def on_queue_bind_ok(self, unused_frame):
        LOG.debug('queue is banded, setup delivery confirmation')
        self._channel.confirm_delivery(self.on_received_or_rejected)

    def on_received_or_rejected(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOG.debug('delivery received or rejected,  %s for delivery tag: %i',
                  confirmation_type,
                  method_frame.method.delivery_tag)

    def publish(self, notification, routing=None):
        if not notification:
            return

        try:
            message = json.dumps(notification, ensure_ascii=False)
            with self._lock:
                self._messages.put(
                    {'routing': str(routing), 'body': str(message)})
        except Exception as e:
            LOG.warning(e)

    def thread_publish(self):
        while not self._stopping:
            if self._channel is None or not self._channel.is_open:
                continue

            try:
                with self._lock:

                    message = self._messages.get()
                    if message is None:
                        continue

                    self._channel.basic_publish(exchange=self._exchange,
                                                routing_key=str(
                                                    message['routing']),
                                                body=str(message['body']))
                    LOG.debug('message is published : ' + str(message))
            except Exception as e:
                LOG.exception(str(e))
        LOG.debug('publish thread has stopped')

    def start(self):
        self._thread_ioloop = threading.Thread(target=self.thread_ioloop,
                                               name="ioloop thread")
        self._thread_ioloop.daemon = True
        self._thread_publish = threading.Thread(target=self.thread_publish,
                                                name="publish thread")
        self._thread_publish.daemon = True

        self._thread_ioloop.start()
        self._thread_publish.start()

        LOG.debug('wait ioloop thread and publish thread become alive')
        while not self._started:
            if self._thread_ioloop.is_alive and self._thread_publish.is_alive:
                self._started = True
        LOG.debug('published has started')

    def thread_ioloop(self):
        while not self._stopping:
            try:
                self._connection = None
                self._connection = self.connect()
                # ioloop is a blocking call
                LOG.debug('start ioloop at ' + str(datetime.datetime.utcnow()))
                self._connection.ioloop.start()
            except Exception as e:
                LOG.warning(str(e))
                self.stop()
                if self._connection is not None and not self._connection.is_closed:
                    self._connection.ioloop.start()

        LOG.debug('ioloop thread has stopped')

    def stop(self):
        LOG.debug('###stopping publisher')
        self._stopping = True
        if self._thread_ioloop is not None:
            LOG.debug('abort ioloop thread')
            self._thread_ioloop.join(0.5)
            self._thread_ioloop = None
        if self._thread_publish is not None:
            LOG.debug('abort publish thread')
            self._thread_publish.join(0.5)
            self._thread_publish = None

        self.close_channel()
        self.close_connection()
        self._started = False

    def close_channel(self):
        if self._channel is not None:
            LOG.debug('close channel')
            self._channel.close()

    def close_connection(self):
        if self._connection is not None:
            LOG.debug('close connection')
            self._connection.ioloop.stop()
            self._connection.close()

    def is_started(self):
        return self._started
