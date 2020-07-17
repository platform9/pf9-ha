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
import datetime
import traceback
import time
from shared.exceptions import ha_exceptions
from shared.rpc.rpc_manager import RpcManager
from shared.messages import message_types
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)

_rebalance_controller = None


class RebalanceController(object):
    rpc_manager = None

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(RebalanceController, cls).__new__(cls)
        return cls.instance

    def __init__(self, config):
        host = None
        port = None
        username = None
        password = None
        exchange = None
        exchange_type = None

        is_rebalance_enabled = False
        if config.has_option("DEFAULT", "enable_consul_role_rebalance"):
            is_rebalance_enabled = config.getboolean("DEFAULT",
                                                     "enable_consul_role_rebalance")

        # queue name for controller to get response need to be unique (when
        # hosts send response, they don't need to know the queue name for
        # their response, they only need to send reponse with the routing key
        # for resp) so make sure the queue name is unique
        queue_for_receiving = 'queue-receiving-for-hamgr'
        if not is_rebalance_enabled:
            return
            # settings from 'amqp' for rabbitmq
        section = 'amqp'
        if config.has_section(section):
            host = config.get(section, 'host')
            port = config.get(section, 'port')
            username = config.get(section, 'username')
            password = config.get(section, 'password')
            virtual_host = config.get(section, 'virtual_host') \
                if config.has_option(section, 'virtual_host') else '/'

        error = "empty value for %s in section " + section
        if not host:
            raise ha_exceptions.ConfigException(error % 'host')
        if not username:
            raise ha_exceptions.ConfigException(error % 'username')
        if not password:
            raise ha_exceptions.ConfigException(error % 'password')
        if not virtual_host:
            raise ha_exceptions.ConfigException(error % 'virtual_host')

        # settings from 'consul_rebalance' for rebalance
        section = 'consul_rebalance'
        if config.has_section(section):
            exchange = config.get(section, 'exchange_name')
            exchange_type = config.get(section, 'exchange_type')
            routingkey_for_sending = 'sending'
            if config.has_option(section, 'routingkey_for_sending'):
                routingkey_for_sending = config.get(section,
                                                    'routingkey_for_sending')

            routingkey_for_receiving = 'receiving'
            if config.has_option(section, 'routingkey_for_receiving'):
                routingkey_for_receiving = config.get(section,
                                                      'routingkey_for_receiving')

        error = "empty value for %s in section " + section
        if not exchange:
            raise ha_exceptions.ConfigException(error % 'exchange_name')
        if not exchange_type:
            raise ha_exceptions.ConfigException(error % 'exchange_type')

        # on controller side , the request will be sending to routing key
        # for requests and receive responses from reponse queue (hosts
        # send responses to routing key for responses) need to make sure
        # exchange type to be 'direct'
        if exchange_type != 'direct':
            LOG.warning("configured exchange type is %s, now force it to direct",
                        exchange_type)
            exchange_type = 'direct'

        if self.rpc_manager:
            return
        self.rpc_manager = RpcManager(host,
                                      port,
                                      username,
                                      password,
                                      virtual_host,
                                      exchange,
                                      exchange_type,
                                      routingkey_for_sending,
                                      queue_for_receiving,
                                      routingkey_for_receiving,
                                      application='RebalanceController')
        # subscribe to consul role rebalance response
        self._received_role_rebalance_responses = []
        self.rpc_manager.subscribe_message(
            message_types.MSG_ROLE_REBALANCE_RESPONSE,
            self.on_consul_role_rebalance_response)
        # subscribe to consul status refresh response
        self._received_status_responses = []
        self.rpc_manager.subscribe_message(
            message_types.MSG_CONSUL_REFRESH_RESPONSE,
            self.on_consul_status_response)

    def __del__(self):
        if self.rpc_manager:
            self.rpc_manager = None
        self._received_role_rebalance_responses = []
        self._received_status_responses = []

    def on_consul_role_rebalance_response(self, response):
        if not response:
            return
        msg_type = response['type']
        if msg_type != message_types.MSG_ROLE_REBALANCE_RESPONSE:
            return
        self._received_role_rebalance_responses.append(response)

    def on_consul_status_response(self, response):
        if not response:
            return
        msg_type = response['type']
        if msg_type != message_types.MSG_CONSUL_REFRESH_RESPONSE:
            return
        self._received_status_responses.append(response)

    def rebalance_and_wait_for_result(self, request):
        LOG.info('sending rebalance request %s', str(request))
        req_id = request['id']
        if not self.rpc_manager:
            LOG.warning(
                'unable to get result as the rebalancer manager is None')
            return None
        self.rpc_manager.send_rpc_message(request, message_type=message_types.MSG_ROLE_REBALANCE_REQUEST)
        # in asynchronous mode, need to wait till response arrive or timed out
        resp = self._wait_for_response_or_timeout(self._received_role_rebalance_responses,
                                                  req_id,
                                                  timeout_seconds=180)
        LOG.info('response for rebalance request %s : %s', req_id, str(resp))
        return resp

    def _wait_for_response_or_timeout(self, receive_buffer, request_id, timeout_seconds = 60):
        time_start = datetime.datetime.utcnow()
        time_delta = datetime.timedelta(seconds=timeout_seconds)
        payload = None
        while True:
            if datetime.datetime.utcnow() - time_start > time_delta:
                LOG.error('response not received after %s '
                          'seconds for request %s',
                          str(timeout_seconds), str(request_id))
                return None
            size = len(receive_buffer)
            if not size:
                continue
            LOG.debug('size of receive_buffer : %s', str(size))
            shadow_buffer = receive_buffer[:]
            for i in range(0, size):
                # index with the shadow buffer to avoid out of index,
                # because we will remove items from receive buffer.
                payload = shadow_buffer[i]
                LOG.debug('response received : %s', str(payload))
                payload = payload
                req_id = payload['req_id']
                if req_id == request_id:
                    LOG.debug('response for request id %s found : %s',
                              request_id, str(payload))
                    receive_buffer.remove(payload)
                    shadow_buffer = []
                    return payload
                # if message in buffer is staled, also remove it
                when = datetime.datetime.strptime(payload['timestamp'],
                                         '%Y-%m-%d %H:%M:%S')
                if (datetime.datetime.utcnow() - when) > time_delta:
                    LOG.debug('remove received message that is older than %s '
                              'seconds : %s', str(timeout_seconds),
                              str(payload))
                    try:
                        receive_buffer.remove(payload)
                    except ValueError:
                        pass

        LOG.debug('response found for request id %s : %s', str(request_id), str(payload))
        return payload

    def ask_for_consul_cluster_status(self, request):
        # send a consul cluster status update request and wait for response
        if not self.rpc_manager:
            LOG.warning(
                'unable to ask for consul cluster status, as rebalancer manager is null')
            return {}
        LOG.debug('send consul refresh request begin at %s : %s',
                  str(datetime.datetime.utcnow()), str(request))
        self.rpc_manager.send_rpc_message(request,
                                          message_type=message_types.MSG_CONSUL_REFRESH_REQUEST)
        resp = self._wait_for_response_or_timeout(self._received_status_responses, request.id(), timeout_seconds=180)
        LOG.debug('consul refresh response received at %s : %s',
                  str(datetime.datetime.utcnow()), str(resp))
        return resp


def get_rebalance_controller(config):
    global _rebalance_controller
    if _rebalance_controller is None:
        LOG.debug('creating rebalance controller')
        LOG.debug(traceback.format_exc())
        _rebalance_controller = RebalanceController(config)
    return _rebalance_controller
