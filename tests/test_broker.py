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
                self.assertIn(client.session.client_id, broker._sessions)
                yield from client.disconnect()
                yield from asyncio.sleep(0.1)
                yield from broker.shutdown()
                self.assertTrue(broker.transitions.is_stopped())
                self.assertDictEqual(broker._sessions, {})
                MockPluginManager.assert_has_calls(
                    [call().fire_event(EVENT_BROKER_CLIENT_CONNECTED, client_id=client.session.client_id),
                     call().fire_event(EVENT_BROKER_CLIENT_DISCONNECTED, client_id=client.session.client_id)],
                    any_order=True)
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()

    @patch('hbmqtt.broker.PluginManager')
    def test_client_subscribe(self, MockPluginManager):
        def test_coro():
            try:
                broker = Broker(test_config, plugin_namespace="hbmqtt.test.plugins")
                yield from broker.start()
                self.assertTrue(broker.transitions.is_started())
                client = MQTTClient()
                ret = yield from client.connect('mqtt://localhost/')
                self.assertEqual(ret, 0)
                yield from client.subscribe([('/topic', QOS_0)])

                # Test if the client test client subscription is registered
                self.assertIn('/topic', broker._subscriptions)
                subs = broker._subscriptions['/topic']
                self.assertEquals(len(subs), 1)
                (s, qos) = subs[0]
                self.assertEquals(s, client.session)
                self.assertEquals(qos, QOS_0)

                yield from client.disconnect()
                yield from asyncio.sleep(0.1)
                yield from broker.shutdown()
                self.assertTrue(broker.transitions.is_stopped())
                MockPluginManager.assert_has_calls(
                    [call().fire_event(EVENT_BROKER_CLIENT_SUBSCRIBED,
                                       client_id=client.session.client_id,
                                       topic='/topic', qos=QOS_0)], any_order=True)
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()

    @patch('hbmqtt.broker.PluginManager')
    def test_client_subscribe_twice(self, MockPluginManager):
        def test_coro():
            try:
                broker = Broker(test_config, plugin_namespace="hbmqtt.test.plugins")
                yield from broker.start()
                self.assertTrue(broker.transitions.is_started())
                client = MQTTClient()
                ret = yield from client.connect('mqtt://localhost/')
                self.assertEqual(ret, 0)
                yield from client.subscribe([('/topic', QOS_0)])

                # Test if the client test client subscription is registered
                self.assertIn('/topic', broker._subscriptions)
                subs = broker._subscriptions['/topic']
                self.assertEquals(len(subs), 1)
                (s, qos) = subs[0]
                self.assertEquals(s, client.session)
                self.assertEquals(qos, QOS_0)

                yield from client.subscribe([('/topic', QOS_0)])
                self.assertEquals(len(subs), 1)
                (s, qos) = subs[0]
                self.assertEquals(s, client.session)
                self.assertEquals(qos, QOS_0)

                yield from client.disconnect()
                yield from asyncio.sleep(0.1)
                yield from broker.shutdown()
                self.assertTrue(broker.transitions.is_stopped())
                MockPluginManager.assert_has_calls(
                    [call().fire_event(EVENT_BROKER_CLIENT_SUBSCRIBED,
                                       client_id=client.session.client_id,
                                       topic='/topic', qos=QOS_0)], any_order=True)
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()

    @patch('hbmqtt.broker.PluginManager')
    def test_client_unsubscribe(self, MockPluginManager):
        def test_coro():
            try:
                broker = Broker(test_config, plugin_namespace="hbmqtt.test.plugins")
                yield from broker.start()
                self.assertTrue(broker.transitions.is_started())
                client = MQTTClient()
                ret = yield from client.connect('mqtt://localhost/')
                self.assertEqual(ret, 0)
                yield from client.subscribe([('/topic', QOS_0)])

                # Test if the client test client subscription is registered
                self.assertIn('/topic', broker._subscriptions)
                subs = broker._subscriptions['/topic']
                self.assertEquals(len(subs), 1)
                (s, qos) = subs[0]
                self.assertEquals(s, client.session)
                self.assertEquals(qos, QOS_0)

                yield from client.unsubscribe(['/topic'])
                self.assertNotIn('/topic', broker._subscriptions)
                yield from client.disconnect()
                yield from asyncio.sleep(0.1)
                yield from broker.shutdown()
                self.assertTrue(broker.transitions.is_stopped())
                MockPluginManager.assert_has_calls(
                    [call().fire_event(EVENT_BROKER_CLIENT_SUBSCRIBED,
                                       client_id=client.session.client_id,
                                       topic='/topic', qos=QOS_0),
                     call().fire_event(EVENT_BROKER_CLIENT_UNSUBSCRIBED,
                                       client_id=client.session.client_id,
                                       topic='/topic')
                    ], any_order=True)
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()
