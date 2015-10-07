# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
from unittest.mock import patch, call
import asyncio
import logging
from hbmqtt.broker import *
from hbmqtt.mqtt.constants import *
from hbmqtt.client import MQTTClient

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=formatter)
log = logging.getLogger(__name__)

test_config = {
    'listeners': {
        'default': {
            'type': 'tcp',
            'bind': 'localhost:1883',
            'max_connections': 10
        },
    },
    'sys_interval': 0,
    'auth': {
        'allow-anonymous': True,
    }
}


class BrokerTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    @patch('hbmqtt.broker.PluginManager')
    def test_start_stop(self, MockPluginManager):
        def test_coro():
            try:
                broker = Broker(test_config, plugin_namespace="hbmqtt.test.plugins")
                yield from broker.start()
                self.assertTrue(broker.transitions.is_started())
                self.assertDictEqual(broker._sessions, {})
                self.assertIn('default', broker._servers)
                MockPluginManager.assert_has_calls(
                    [call().fire_event(EVENT_BROKER_PRE_START),
                     call().fire_event(EVENT_BROKER_POST_START)], any_order=True)
                MockPluginManager.reset_mock()
                yield from broker.shutdown()
                MockPluginManager.assert_has_calls(
                    [call().fire_event(EVENT_BROKER_PRE_SHUTDOWN),
                     call().fire_event(EVENT_BROKER_POST_SHUTDOWN)], any_order=True)
                self.assertTrue(broker.transitions.is_stopped())
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()

    @patch('hbmqtt.broker.PluginManager')
    def test_client_connect(self, MockPluginManager):
        def test_coro():
            try:
                broker = Broker(test_config, plugin_namespace="hbmqtt.test.plugins")
                yield from broker.start()
                self.assertTrue(broker.transitions.is_started())
                client = MQTTClient()
                ret = yield from client.connect('mqtt://localhost/')
                self.assertEqual(ret, 0)
                yield from client.disconnect()
                yield from asyncio.sleep(0.1)
                yield from broker.shutdown()
                self.assertTrue(broker.transitions.is_stopped())
                MockPluginManager.assert_has_calls(
                    [call().fire_event(EVENT_BROKER_CLIENT_CONNECTED, session=client.session),
                     call().fire_event(EVENT_BROKER_CLIENT_DISCONNECTED, session=client.session)], any_order=True)
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()
