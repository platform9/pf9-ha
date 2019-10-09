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

from shared.exceptions import ha_exceptions
from shared.rebalance.manager import RebalanceManager
from shared.messages import message_types

LOG = logging.getLogger(__name__)

_rebalance_controller = None


class RebalanceController(object):
    rebalancer_manager = None

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(RebalanceController, cls).__new__(cls)
        return cls.instance

    def __init__(self, config):
        is_rebalance_enabled = config.getboolean("DEFAULT", "enable_consul_role_rebalance") \
            if config.has_option("DEFAULT", "enable_consul_role_rebalance") else False

        host = None
        port = None
        username = None
        password = None
        exchange = None
        exchange_type = None
        # queue name for controller to get response need to be unique (when hosts send response, they don't need
        # to know the queue name for their response, they only need to send reponse with the routing key for resp)
        # so make sure the queue name is unique
        queue_for_receiving = 'queue-receiving-for-hamgr'
        if is_rebalance_enabled:
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
                routingkey_for_sending = config.get(section, 'routingkey_for_sending') \
                    if config.has_option(section, 'routingkey_for_sending') else 'sending'
                routingkey_for_receiving = config.get(section, 'routingkey_for_receiving') \
                    if config.has_option(section, 'routingkey_for_receiving') else 'receiving'

            error = "empty value for %s in section " + section
            if not exchange:
                raise ha_exceptions.ConfigException(error % 'exchange_name')
            if not exchange_type:
                raise ha_exceptions.ConfigException(error % 'exchange_type')

            # on controller side , the request will be sending to routing key for requests
            # and receive responses from reponse queue (hosts send responses to routing key for responses)
            # need to make sure exchange type to be 'direct'
            if exchange_type != 'direct':
                LOG.warn('configured exchange type is %s, now force it to direct', exchange_type)
                exchange_type = 'direct'

            if self.rebalancer_manager is None:
                self.rebalancer_manager = RebalanceManager(host,
                                                           port,
                                                           username,
                                                           password,
                                                           virtual_host,
                                                           exchange,
                                                           exchange_type,
                                                           routingkey_for_sending,
                                                           queue_for_receiving,
                                                           routingkey_for_receiving)

    def __del__(self):
        if self.rebalancer_manager:
            self.rebalancer_manager = None

    def rebalance_and_wait_for_result(self, request):
        LOG.debug('sending rebalance request %s', str(request))
        req_id = request['id']
        if not self.rebalancer_manager:
            LOG.warn('unable to get result as the rebalancer manager is None')
            return None
        self.rebalancer_manager.send_role_rebalance_request(request)
        resp = self.rebalancer_manager.get_role_rebalance_response(req_id)
        LOG.debug('response for request %s : %s', req_id, str(resp))
        return resp

    def ask_for_consul_cluster_status(self, request):
        # send a consul cluster status update request and wait for response
        if not self.rebalancer_manager:
            LOG.warn('unable to ask for consul cluster status, as rebalancer manager is null')
            return {}
        LOG.debug('send consul refresh request begin at %s', str(datetime.datetime.utcnow()))
        self.rebalancer_manager.send_role_rebalance_request(request, type=message_types.MSG_CONSUL_REFRESH_REQUEST)
        resp = self.rebalancer_manager.get_role_rebalance_response(request.id(),
                                                                   response_type=message_types.MSG_CONSUL_REFRESH_RESPONSE,
                                                                   timeout_seconds=120)
        LOG.debug('consul refresh response received at %s : %s', str(datetime.datetime.utcnow()), str(resp))
        return resp


def get_rebalance_controller(config):
    global _rebalance_controller
    if _rebalance_controller is None:
        LOG.debug('creating rebalance controller')
        LOG.debug(traceback.format_exc())
        _rebalance_controller = RebalanceController(config)
    return _rebalance_controller
