from ConfigParser import ConfigParser
import sys
import unittest
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("pika")
for hdr in logger.handlers:
    logger.removeHandler(hdr)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

import time


@unittest.skip('tests requires rabbitmq server runs locally')
class RpcTest(unittest.TestCase):
    _producer = None
    _consumer = None
    _client = None
    _host = '127.0.0.1'
    _port = 5672
    _user = 'admin'
    _password = 'admin'
    _exchange = 'pf9-vmha-exchanges'
    _exchange_type = 'topic'
    _queue_name = 'pf9-vmha'
    _virtualhost = '/'
    _routingkey = 'test'

    def setUp(self):
        self.result = None

    def callback(self, msg):
        logger.debug('receiving %s', str(msg))
        print('receiving - ' + str(msg))
        self.result = msg

    def test_single_channel(self):
        from shared.rpc.rpc_dual_client import RpcDualClient
        client = RpcDualClient(self._host,
                               self._port,
                               self._user,
                               self._password,
                               self._exchange,
                               self._exchange_type,
                               self._queue_name,
                               self._routingkey)
        client.consume(self.callback)
        client.start()
        i = 0
        while not self.result:
            message = {'key_%s' % str(i): 'value_%s' % str(i)}
            client.publish(message, routing=self._routingkey)
            print('publishing : ' + str(message))
            logger.debug('published : %s', str(message))
            time.sleep(0.500)
            i = i + 1
        logger.debug('consumed : %s', str(self.result))
        client.stop()

    def test_two_channels(self):
        from shared.rpc.rpc_producer import RpcProducer
        from shared.rpc.rpc_consumer import RpcConsumer
        producer = RpcProducer(self._host,
                               self._port,
                               self._user,
                               self._password,
                               self._exchange,
                               self._exchange_type,
                               self._routingkey)
        producer.start()
        consumer = RpcConsumer(self._host,
                               self._port,
                               self._user,
                               self._password,
                               self._exchange,
                               self._exchange_type,
                               self._queue_name,
                               self._routingkey)
        consumer.consume(self.callback)
        self.result = None
        consumer.start()
        i = 0
        while not self.result:
            message = {'key_%s' % str(i): 'value_%s' % str(i)}
            producer.publish(message, routing=self._routingkey)
            print('publishing : ' + str(message))
            logger.debug('published : %s', str(message))
            time.sleep(0.500)
            i = i + 1
        logger.debug('consumed : %s', str(self.result))
        consumer.stop()
        producer.stop()
