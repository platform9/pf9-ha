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
import pika
import threading
import datetime
import time

LOG = logging.getLogger(__name__)


class RpcBase(object):
    def __init__(self,
                 host,
                 port,
                 user,
                 password,
                 exchange,
                 exchange_type,
                 virtual_host="/"):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._virtual_host = virtual_host

        self._connection = None
        self._channel = None
        self._stopping = False
        self._credentials = pika.PlainCredentials(username=self._user, password=self._password)
        self._connection_parameters = pika.ConnectionParameters(host=self._host,
                                                                port=int(self._port),
                                                                virtual_host=self._virtual_host,
                                                                credentials=self._credentials)
        self._closing = False
        self._connection_ready = False

    def _open_connection(self):
        LOG.debug('connecting to amqp://%s:%s@%s:%s/', self._user, self._password, self._host, str(self._port))
        return pika.SelectConnection(self._connection_parameters,
                                     on_open_callback=self._on_connection_open,
                                     on_open_error_callback=self._on_connection_open_error,
                                     on_close_callback=self._on_connection_closed,
                                     stop_ioloop_on_close=False)

    def _close_connection(self):
        LOG.debug('closing connection')
        self._closing = True
        if self._connection is not None:
            self._connection.close()

    def _on_connection_open_error(self, _unused_connection, err):
        LOG.error('error when open connection, %s', str(err))
        LOG.debug('reconnect when unable to open connection')
        time.sleep(5)
        self._connection.ioloop.stop()

    def _on_connection_closed(self, connection, reply_code, reply_text):
        LOG.debug('connection was closed')
        self._channel = None
        if self._closing:
            LOG.debug('stop ioloop since connection was closed.')
            self._connection.ioloop.stop()
        else:
            LOG.debug('connection already closed, not stop connection ioloop')
            LOG.warning('connection was closed, will reopen. error status : (%s) %s', reply_code, reply_text)
            time.sleep(5)
            self._connection.ioloop.stop()

    def _on_connection_open(self, unused_connection):
        LOG.debug('connection opened')
        self._open_channel()

    def _open_channel(self):
        LOG.debug('creating a new channel')
        self._connection.channel(on_open_callback=self._on_channel_open)

    def close_channel(self):
        LOG.debug('closing the channel')
        if self._channel is not None:
            self._channel.close()

    def _add_on_channel_close_callback(self):
        LOG.debug('adding channel close callback')
        self._channel.add_on_close_callback(self._on_channel_closed)

    def _on_channel_closed(self, channel, reply_code, reply_text):
        LOG.warning('channel was closed: (%s) %s', reply_code, reply_text)
        if not self._closing:
            LOG.debug('close connection since channel was closed')
            self._connection.close()
        else:
            LOG.debug('connection already closed when channel was closed ? %s', str(self._connection.is_closed))

    def _on_channel_open(self, channel):
        LOG.debug('channel opened')
        self._channel = channel
        self._add_on_channel_close_callback()
        # once channel is created, both producer and consumer need to declare exchange
        self._setup_exchange(self._exchange)

    def _setup_exchange(self, exchange_name):
        LOG.debug('declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self._on_exchange_declare_ok,
                                       exchange_name,
                                       self._exchange_type)

    def _on_exchange_declare_ok(self, unused_frame):
        LOG.debug('exchange declared')
        # up to exchange declared, for producer , no need to declare and bind queue, but consumer need to
        self.on_connection_ready()
        self._connection_ready = True

    def _run(self):
        while not self._stopping:
            self._connection = None
            try:
                self._connection = self._open_connection()
                self._connection.ioloop.start()
            except Exception as e:
                LOG.exception('unhandled exception for ioloop : %s', str(e))
                time.sleep(5)
        LOG.debug('IOLoop stopped')

    def start(self):
        LOG.debug('start client')
        self._ioloop_thread = threading.Thread(target=self._run)
        self._ioloop_thread.start()

        # wait for connection ready, or timeout in 180 seconds
        ready = True
        timeout = datetime.timedelta(seconds=60)
        time_start = datetime.datetime.utcnow()
        while not self.is_connected():
            time.sleep(.100)
            if datetime.datetime.utcnow() - time_start > timeout:
                ready = False
                break
        if ready:
            LOG.debug('client started')
        else:
            LOG.warning('connection is not established in 180 seconds')

    def stop(self):
        LOG.debug('stopping client')
        self._stopping = True
        self._closing = True

        if self._ioloop_thread:
            self._ioloop_thread.join(.100)
            self._ioloop_thread = None
        self.on_stopping()
        self.close_channel()
        self._close_connection()
        self._connection_ready = False
        LOG.debug('client stopped')

    def is_connected(self):
        #return self._connection_ready
        return self.is_setup_ready()

    def is_stopping(self):
        return self._stopping

    def get_connection(self):
        return self._connection

    def get_channel(self):
        return self._channel

    def on_connection_ready(self):
        pass

    def on_stopping(self):
        pass

    def is_setup_ready(self):
        pass
