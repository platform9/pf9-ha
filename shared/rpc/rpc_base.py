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
                                                                port=self._port,
                                                                virtual_host=self._virtual_host,
                                                                credentials=self._credentials)
        self._closing = False
        self._connection_ready = False

    def _open_connection(self):
        LOG.info('connecting to amqp://%s:%s@%s:%s/', self._user, self._password, self._host, str(self._port))
        return pika.SelectConnection(self._connection_parameters,
                                     on_open_callback=self._on_connection_open,
                                     on_open_error_callback=self._on_connection_open_error,
                                     on_close_callback=self._on_connection_closed,
                                     stop_ioloop_on_close=False)

    def _close_connection(self):
        LOG.info('closing connection')
        self._closing = True
        self._connection.close()

    def _on_connection_open_error(self, _unused_connection, err):
        LOG.error('error when open connection, %s', str(err))
        LOG.info('reconnect when unable to open connection')
        self._reconnect()

    def _on_connection_closed(self, connection, reply_code, reply_text):
        LOG.info('connection was closed')
        self._channel = None
        if self._closing:
            LOG.info('stop ioloop since connection was closed.')
            self._connection.ioloop.stop()
        else:
            LOG.info('connection already closed, not stop connection ioloop')
            LOG.warning('connection was closed, will reopen. error status : (%s) %s', reply_code, reply_text)
            self._reconnect()

    def _on_connection_open(self, unused_connection):
        LOG.info('connection opened')
        self._open_channel()

    def _open_channel(self):
        LOG.info('creating a new channel')
        self._connection.channel(on_open_callback=self._on_channel_open)

    def close_channel(self):
        LOG.info('closing the channel')
        if self._channel and not self._channel.is_closing:
            self._channel.close()

    def _reconnect(self):
        LOG.info('try to reconnect')
        self._connection.ioloop.stop()
        if not self._closing:
            LOG.info('reconnect and start connection ioloop')
            self._connection = self._open_connection()
            self._connection.ioloop.start()
        else:
            LOG.info('not reconnect as connection is still in closing')

    def _add_on_channel_close_callback(self):
        LOG.info('adding channel close callback')
        self._channel.add_on_close_callback(self._on_channel_closed)

    def _on_channel_closed(self, channel, reply_code, reply_text):
        LOG.warning('channel was closed: (%s) %s', reply_code, reply_text)
        if not self._closing:
            LOG.info('close connection since channel was closed')
            self._connection.close()
        else:
            LOG.info('connection already closed when channel was closed ? %s', str(self._connection.is_closed))

    def _on_channel_open(self, channel):
        LOG.info('channel opened')
        self._channel = channel
        self._add_on_channel_close_callback()
        # once channel is created, both producer and consumer need to declare exchange
        self._setup_exchange(self._exchange)

    def _setup_exchange(self, exchange_name):
        LOG.info('declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self._on_exchange_declare_ok,
                                       exchange_name,
                                       self._exchange_type)

    def _on_exchange_declare_ok(self, unused_frame):
        LOG.info('exchange declared')
        # up to exchange declared, for producer , no need to declare and bind queue, but consumer need to
        self.on_connection_ready()
        self._connection_ready = True

    def _run(self):
        self._connection = self._open_connection()
        self._connection.ioloop.start()

    def start(self):
        LOG.info('start client')
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
            LOG.info('client started')
        else:
            LOG.warning('connection is not established in 180 seconds')

    def stop(self):
        LOG.info('stopping client')
        self._stopping = True
        self._closing = True

        if self._ioloop_thread:
            self._ioloop_thread.join(.100)
            self._ioloop_thread = None
        self.on_stopping()
        self.close_channel()
        self._close_connection()
        self._connection.ioloop.start()
        self._connection_ready = False
        LOG.info('client stopped')

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
