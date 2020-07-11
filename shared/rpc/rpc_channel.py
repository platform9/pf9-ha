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
import re
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)


class RpcChannel(object):
    def __init__(self,
                 host,
                 port,
                 user,
                 password,
                 exchange,
                 exchange_type,
                 virtual_host="/",
                 application='',
                 connection_ready_callback=None,
                 connection_close_callback=None):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._virtual_host = virtual_host
        self._application = str(application)

        self._connection = None
        self._channel = None
        self._stopping = False
        self._credentials = pika.PlainCredentials(username=self._user,
                                                  password=self._password)
        self._connection_parameters = pika.ConnectionParameters(host=self._host,
                                                                port=int(
                                                                    self._port),
                                                                virtual_host=self._virtual_host,
                                                                credentials=self._credentials)
        self._closing = False
        self._connection_ready = False
        # the default settings used when declare exchange:
        # exchange_type='direct',
        # passive=False,
        # durable=False,
        # auto_delete=False,
        # amount them, the 'aut_delete' may have different value on rabbitmq
        # so need to handle it when exchange declaration failed
        self._exchange_auto_delete = False

        # callbacks
        self._connection_ready_callback = None
        self._connection_close_callback = None
        if connection_ready_callback and callable(connection_ready_callback):
            self._connection_ready_callback = connection_ready_callback
        if connection_close_callback and callable(connection_close_callback):
            self._connection_close_callback = connection_close_callback

    def _open_connection(self):
        LOG.debug('create RPC pika connecting to amqp://password:%s@%s:%s/ for %s', self._user,
                  self._host, str(self._port), self._application)
        return pika.SelectConnection(self._connection_parameters,
                                     on_open_callback=self._on_connection_open,
                                     on_open_error_callback=self._on_connection_open_fault,
                                     on_close_callback=self._on_connection_closed)

    def _close_connection(self):
        LOG.debug('closing RPC pika connection for %s', self._application)
        self._closing = True
        if self._connection is not None:
            self._connection.close()

    def _on_connection_open_fault(self, _unused_connection, err):
        LOG.debug('error when open RPC pika connection for %s, will reopen : %s',
                  self._application, str(err))
        time.sleep(5)
        if self._connection:
            self._connection.ioloop.stop()

    def _on_connection_closed(self, connection, reply_code, reply_text):
        LOG.debug('RPC pika connection was closed for %s', self._application)
        if self._closing:
            LOG.debug('stop RPC pika ioloop for %s since connection was closed.', self._application)
            self._connection.ioloop.stop()
        else:
            time.sleep(5)
            LOG.debug('RPC pika connection for %s was closed, will reopen. error : %s %s',
                self._application, reply_code, reply_text)
            self.close_channel()
            self._connection.ioloop.stop()

    def _on_connection_open(self, unused_connection):
        LOG.debug('RPC pika connection opened for %s', self._application)
        self._open_channel()

    def _open_channel(self):
        LOG.debug('creating RPC pika channel for %s', self._application)
        self._connection.channel(on_open_callback=self._on_channel_open)

    def close_channel(self):
        LOG.debug('closing RPC pika channel for %s', self._application)
        if self._channel is not None:
            self._channel.close()

    def _add_on_channel_close_callback(self):
        LOG.debug('adding RPC pika channel close callback for %s', self._application)
        self._channel.add_on_close_callback(self._on_channel_closed)

    def _on_channel_closed(self, channel, reply_code, reply_text):
        LOG.warning('RPC pika channel for %s was closed: %s , %s',
                    self._application,
                    str(reply_code),
                    str(reply_text))
        # when declare exchange failed, will get error like this :
        #  replay_code = 406 ,
        # reply_text = PRECONDITION_FAILED - inequivalent arg 'auto_delete' for
        # exchange 'pf9-changes' in vhost '/': received 'false'
        # but current is 'true'
        if reply_code == 406:
            pattern = re.compile(r".*inequivalent arg 'auto_delete' for exchange.*in vhost.*:.*received '(?P<local>\w+)' but current is '(?P<remote>\w+)'.*")
            matched = re.match(pattern, reply_text)
            if matched:
                local = bool(matched.group('local'))
                remote = bool(matched.group('remote'))
                LOG.debug("'auto_delete' for exchange %s on rabbitmq server "
                          "is '%s', but we declared it with '%s'",
                          self._exchange, str(remote), str(local))
                if remote != self._exchange_auto_delete:
                    LOG.debug('retry exchange declaration with auto_delete as '
                              '%s',str(self._exchange_auto_delete))
                    time.sleep(5)
                    self._setup_exchange()
                    return
        if not self._closing:
            LOG.debug('close RPC pika connection for %s since channel was closed', self._application)
            self._connection.close()
        else:
            LOG.debug('RPC pika connection for %s already closed when channel was closed ? %s',
                      self._application, str(self._connection.is_closed))

    def _on_channel_open(self, channel):
        LOG.debug('RPC pika channel has opened for %s', self._application)
        self._channel = channel
        self._add_on_channel_close_callback()
        # once channel is created, both producer and consumer need to declare
        # exchange
        self._setup_exchange()

    def _setup_exchange(self):
        LOG.debug('RPC pika channel is declaring exchange %s for %s', self._exchange, self._application)
        try:
            self._channel.exchange_declare(self._on_exchange_declare_ok,
                                           exchange=self._exchange,
                                           exchange_type=self._exchange_type,
                                           auto_delete=self._exchange_auto_delete)
        except Exception:
            LOG.exception('unhandled exchange declaration exception for %s', self._application)

    def _on_exchange_declare_ok(self, unused_frame):
        LOG.debug('RPC pika channel exchange declared for %s', self._application)
        try:
            # call the callback if exists
            if self._connection_ready_callback:
                self._connection_ready_callback()
        except Exception:
            LOG.exception('unhandled exception from connection_ready_callback for %s', self._application)
        self._connection_ready = True

    def _run(self):
        LOG.debug('RPC pika channel ioloop thread started for %s', self._application)
        interval_seconds = 5
        while not self._stopping:
            if self._connection:
                self._connection.close()
                self._connection = None
                time.sleep(interval_seconds)
            try:
                self._connection = self._open_connection()
                self._connection.ioloop.start()
            except Exception :
                LOG.exception('unhandled RPC pika connection ioloop exception for %s', self._application)
                LOG.debug('restart RPC pika connection for %s in %s seconds', self._application, str(interval_seconds))
                self._connection.ioloop.stop()
                self._close_connection()
            LOG.debug('will restart RPC pika connection and ioloop for %s in %s seconds', self._application, str(interval_seconds))
            time.sleep(interval_seconds)
        LOG.debug('RPC pika channel IOLoop thread stopped for %s', self._application)

    def is_channel_ready(self):
        return self._connection_ready

    def start(self):
        LOG.debug('starting RPC pika channel for %s', self._application)
        self._ioloop_thread = threading.Thread(name='RPCPikaIoloop', target=self._run)
        self._ioloop_thread.start()

        # wait for connection ready, or timeout
        timeout_seconds = 120
        ready = True
        timeout = datetime.timedelta(seconds=timeout_seconds)
        time_start = datetime.datetime.utcnow()
        while not self.is_channel_ready():
            time.sleep(.100)
            if datetime.datetime.utcnow() - time_start > timeout:
                ready = False
                break
        if ready:
            LOG.info('RPC pika channel started for %s', self._application)
        else:
            LOG.warning('RPC pika channel for %s has not started in %s seconds',
                        self._application, str(timeout_seconds))

    def stop(self):
        LOG.debug('stopping RPC pika channel for %s', self._application)
        self._stopping = True
        self._closing = True

        if self._ioloop_thread:
            self._ioloop_thread.join(.100)
            self._ioloop_thread = None
        try:
            # call the callback if exists
            if self._connection_close_callback:
                self._connection_close_callback()
        except Exception:
            LOG.exception('unhandled exception from connection_ready_callback for %s', self._application)
        self.close_channel()
        self._close_connection()
        self._connection_ready = False
        LOG.debug('RPC pika channel stopped for %s', self._application)

    def get_connection(self):
        return self._connection

    def get_channel(self):
        return self._channel

    def add_connection_ready_callback(self, callback):
        if not callback or not callable(callback):
            raise Exception('callback is null or not callable method')
        self._connection_ready_callback = callback

    def add_connection_close_callback(self, callback):
        if not callback or not callable(callback):
            raise Exception('callback is null or not callable method')
        self._connection_close_callback = callback

    def restart(self):
        if self._connection:
            self._connection.ioloop.stop()
