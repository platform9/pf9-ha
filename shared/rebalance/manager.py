import logging
import Queue
import time
import datetime
from shared.exceptions import ha_exceptions
from shared.rpc.rpc_producer import RpcProducer
from shared.rpc.rpc_consumer import RpcConsumer
from shared.messages import message_types
from shared.messages.rebalance_request import ConsulRoleRebalanceRequest
from shared.messages.rebalance_response import ConsulRoleRebalanceResponse

LOG = logging.getLogger(__name__)


class RebalanceManager(object):
    role_rebalance_rpc_producer = None
    role_rebalance_rpc_consumer = None
    rpc_client = None
    role_rebalance_requests_buffer = None
    role_rebalance_responses_buffer = None

    def __init__(self,
                 host,
                 port,
                 username,
                 password,
                 virtual_host,
                 exchange_name,
                 exchange_type,
                 routingkey_for_sending,
                 queue_for_receiving,
                 routingkey_for_receiving):
        self.role_rebalance_requests_buffer = Queue.Queue()
        self.role_rebalance_responses_buffer = Queue.Queue()

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
        if not queue_for_receiving:
            raise ha_exceptions.ArgumentException(error % 'queue_for_receiving')

        # in the rebalance scenario, the controller needs to broadcast request to all hosts
        # the protocol should be like this:
        #  controller - as request producer, setup exchange as 'direct', use routing key for requests when publish
        #               as response consumer, use unique queue name, use routing key for response when consume
        #  hosts      - as request consumer, use unique queue name, use routing key for requests when consume
        #               as response producer, setup exchange as 'direct', use routng key for response when publish
        msg = 'host:%s, port:%s, exchange:%s, exchange key:%s' % (str(host),
                                                                  str(port),
                                                                  str(exchange_name),
                                                                  str(exchange_type))
        if self.role_rebalance_rpc_producer is None:
            LOG.info('create RPC producer, %s, routing %s', msg, routingkey_for_sending)
            self.role_rebalance_rpc_producer = RpcProducer(host=host,
                                                           port=port,
                                                           user=username,
                                                           password=password,
                                                           exchange=exchange_name,
                                                           exchange_type=exchange_type,
                                                           virtual_host=virtual_host,
                                                           routing_key=routingkey_for_sending)
            LOG.info('start RPC producer, %s, routing %s', msg, routingkey_for_sending)
            self.role_rebalance_rpc_producer.start()
        if self.role_rebalance_rpc_consumer is None:
            LOG.info('create RPC consumer, %s, rouging key %s, queue %s', msg, routingkey_for_receiving, queue_for_receiving)
            self.role_rebalance_rpc_consumer = RpcConsumer(host=host,
                                                           port=port,
                                                           user=username,
                                                           password=password,
                                                           exchange=exchange_name,
                                                           exchange_type=exchange_type,
                                                           queue_name=queue_for_receiving,
                                                           virtual_host=virtual_host,
                                                           routing_key=routingkey_for_receiving)
            LOG.info('set rpc consumer message callback')
            self.role_rebalance_rpc_consumer.consume(self._on_consul_role_rebalance_messages)
            LOG.info('start RPC consumer , %s', msg)
            self.role_rebalance_rpc_consumer.start()


    def __del__(self):
        if self.role_rebalance_rpc_producer and self.role_rebalance_rpc_producer.is_connected():
            self.role_rebalance_rpc_producer.stop()
            self.role_rebalance_rpc_producer = None
        if self.role_rebalance_rpc_consumer and self.role_rebalance_rpc_consumer.is_connected():
            self.role_rebalance_rpc_consumer.stop()
            self.role_rebalance_rpc_consumer = None
        self.role_rebalance_requests_buffer = None
        self.role_rebalance_responses_buffer = None

    def _on_consul_role_rebalance_messages(self, message):
        LOG.info('received role rebalance message %s', str(message))
        if not message:
            LOG.warning('received message is empty')
            return
        tag = message['tag']
        payload = message['body']
        if payload:
            type = payload['type']
            LOG.info('payload of type %s : %s', type, str(payload))
            if type == message_types.MSG_ROLE_REBALANCE_REQUEST:
                self.role_rebalance_requests_buffer.put(message)
            if type == message_types.MSG_ROLE_REBALANCE_RESPONSE:
                self.role_rebalance_responses_buffer.put(message)

    def send_role_rebalance_request(self, request):
        if request is None:
            LOG.warn('ignore rebalance request as it is null or empty')
            return
        if not request or not isinstance(request, ConsulRoleRebalanceRequest):
            LOG.warn('ignore rebalance request as it is not type of ConsulRoleRebalanceRequest')
            return

        producer = self.role_rebalance_rpc_producer
        if producer is None:
            LOG.warn('ignore rebalance request as the RPC producer is null')
            return
        if not producer.is_connected():
            LOG.warn('ignore rebalance request as the RPC producer is not connected to server')
            return
        producer.publish(request)
        LOG.info('successfully sent rebalance request')

    def send_role_rebalance_response(self, response):
        if response is None:
            LOG.warn('ignore sending rebalance response as the response is null or empty')
            return
        if not response or not isinstance(response, ConsulRoleRebalanceResponse):
            LOG.warn('ignore sending rebalance response as the response is not type of ConsulRoleRebalanceResponse')
            return

        producer = self.role_rebalance_rpc_producer
        if producer is None:
            LOG.warn('ignore sending rebalance response as the producer is null or empty')
            return
        if not producer.is_connected():
            LOG.warn('ignore sending rebalance response as the producer is not connected to remote server')
            return
        producer.publish(response)
        LOG.info('successfully sent rebalance response')

    def get_role_rebalance_request(self):
        # wait 30 seconds for message if not arrived
        time_start = datetime.datetime.utcnow()
        time_delta = datetime.timedelta(seconds=30)
        while True:
            if datetime.datetime.utcnow() - time_start > time_delta:
                break
            if not self.role_rebalance_requests_buffer.empty():
                item = self.role_rebalance_requests_buffer.get(block=False)
                if item:
                    tag = item['tag']
                    payload = item['body']
                    return payload
        LOG.info('no request found')
        return None

    def get_role_rebalance_response(self, request_id):
        # wait 30 seconds for message if not arrived
        time_start = datetime.datetime.utcnow()
        time_delta = datetime.timedelta(seconds=30)
        while True:
            if not self.role_rebalance_responses_buffer.empty():
                # responses received are put into queue in order, so dequeue not any previously received
                # until the one matches the request
                item = self.role_rebalance_responses_buffer.get(block=False)
                if not item:
                    time.sleep(0.100)
                    continue
                LOG.debug('response received : %s', str(item))
                tag = item['tag']
                payload = item['body']
                req_id = payload['req_id']
                if req_id == request_id:
                    LOG.debug('response for request id %s found : %s', request_id, str(item))
                    return payload
            else:
                if datetime.datetime.utcnow() - time_start > time_delta:
                    LOG.info('rebalance response not received after 30 seconds')
                    break
        LOG.info('no response found for request id %s', str(request_id))
        return None
