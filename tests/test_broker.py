# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
from unittest.mock import patch, call, MagicMock
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


class AsyncMock(MagicMock):
    def __await__(self, *args, **kwargs):
            future = asyncio.Future()
            future.set_result(self)
            result = yield from future
            return result

class BrokerTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    @patch('hbmqtt.broker.PluginManager', new_callable=AsyncMock)
    def test_start_stop(self, MockPluginManager):
        async def test_coro():
            try:
                broker = Broker(test_config, plugin_namespace="hbmqtt.test.plugins")
                await broker.start()
                self.assertTrue(broker.transitions.is_started())
                self.assertDictEqual(broker._sessions, {})
                self.assertIn('default', broker._servers)
                MockPluginManager.assert_has_calls(
                    [call().fire_event(EVENT_BROKER_PRE_START),
                     call().fire_event(EVENT_BROKER_POST_START)], any_order=True)
                MockPluginManager.reset_mock()
                await broker.shutdown()
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

    @patch('hbmqtt.broker.PluginManager', new_callable=AsyncMock)
    def test_client_connect(self, MockPluginManager):
        async def test_coro():
            try:
                broker = Broker(test_config, plugin_namespace="hbmqtt.test.plugins")
                await broker.start()
                self.assertTrue(broker.transitions.is_started())
                client = MQTTClient()
                ret = await client.connect('mqtt://localhost/')
                self.assertEqual(ret, 0)
                self.assertIn(client.session.client_id, broker._sessions)
                await client.disconnect()
                await asyncio.sleep(0.1)
                await broker.shutdown()
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

    @patch('hbmqtt.broker.PluginManager', new_callable=AsyncMock)
    def test_client_subscribe(self, MockPluginManager):
        async def test_coro():
            try:
                broker = Broker(test_config, plugin_namespace="hbmqtt.test.plugins")
                await broker.start()
                self.assertTrue(broker.transitions.is_started())
                client = MQTTClient()
                ret = await client.connect('mqtt://localhost/')
                self.assertEqual(ret, 0)
                await client.subscribe([('/topic', QOS_0)])

                # Test if the client test client subscription is registered
                self.assertIn('/topic', broker._subscriptions)
                subs = broker._subscriptions['/topic']
                self.assertEquals(len(subs), 1)
                (s, qos) = subs[0]
                self.assertEquals(s, client.session)
                self.assertEquals(qos, QOS_0)

                await client.disconnect()
                await asyncio.sleep(0.1)
                await broker.shutdown()
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

    @patch('hbmqtt.broker.PluginManager', new_callable=AsyncMock)
    def test_client_subscribe_twice(self, MockPluginManager):
        async def test_coro():
            try:
                broker = Broker(test_config, plugin_namespace="hbmqtt.test.plugins")
                await broker.start()
                self.assertTrue(broker.transitions.is_started())
                client = MQTTClient()
                ret = await client.connect('mqtt://localhost/')
                self.assertEqual(ret, 0)
                await client.subscribe([('/topic', QOS_0)])

                # Test if the client test client subscription is registered
                self.assertIn('/topic', broker._subscriptions)
                subs = broker._subscriptions['/topic']
                self.assertEquals(len(subs), 1)
                (s, qos) = subs[0]
                self.assertEquals(s, client.session)
                self.assertEquals(qos, QOS_0)

                await client.subscribe([('/topic', QOS_0)])
                self.assertEquals(len(subs), 1)
                (s, qos) = subs[0]
                self.assertEquals(s, client.session)
                self.assertEquals(qos, QOS_0)

                await client.disconnect()
                await asyncio.sleep(0.1)
                await broker.shutdown()
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

    @patch('hbmqtt.broker.PluginManager', new_callable=AsyncMock)
    def test_client_unsubscribe(self, MockPluginManager):
        async def test_coro():
            try:
                broker = Broker(test_config, plugin_namespace="hbmqtt.test.plugins")
                await broker.start()
                self.assertTrue(broker.transitions.is_started())
                client = MQTTClient()
                ret = await client.connect('mqtt://localhost/')
                self.assertEqual(ret, 0)
                await client.subscribe([('/topic', QOS_0)])

                # Test if the client test client subscription is registered
                self.assertIn('/topic', broker._subscriptions)
                subs = broker._subscriptions['/topic']
                self.assertEquals(len(subs), 1)
                (s, qos) = subs[0]
                self.assertEquals(s, client.session)
                self.assertEquals(qos, QOS_0)

                await client.unsubscribe(['/topic'])
                await asyncio.sleep(0.1)
                self.assertEquals(broker._subscriptions['/topic'], [])
                await client.disconnect()
                await asyncio.sleep(0.1)
                await broker.shutdown()
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

    @patch('hbmqtt.broker.PluginManager', new_callable=AsyncMock)
    def test_client_publish(self, MockPluginManager):
        async def test_coro():
            try:
                broker = Broker(test_config, plugin_namespace="hbmqtt.test.plugins")
                await broker.start()
                self.assertTrue(broker.transitions.is_started())
                pub_client = MQTTClient()
                ret = await pub_client.connect('mqtt://localhost/')
                self.assertEqual(ret, 0)

                ret_message = await pub_client.publish('/topic', b'data', QOS_0)
                await pub_client.disconnect()
                self.assertEquals(broker._retained_messages, {})

                await asyncio.sleep(0.1)
                await broker.shutdown()
                self.assertTrue(broker.transitions.is_stopped())
                MockPluginManager.assert_has_calls(
                    [call().fire_event(EVENT_BROKER_MESSAGE_RECEIVED,
                                       client_id=pub_client.session.client_id,
                                       message=ret_message),
                    ], any_order=True)
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()

    @patch('hbmqtt.broker.PluginManager', new_callable=AsyncMock)
    def test_client_publish_retain(self, MockPluginManager):
        async def test_coro():
            try:
                broker = Broker(test_config, plugin_namespace="hbmqtt.test.plugins")
                await broker.start()
                self.assertTrue(broker.transitions.is_started())

                pub_client = MQTTClient()
                ret = await pub_client.connect('mqtt://localhost/')
                self.assertEqual(ret, 0)
                ret_message = await pub_client.publish('/topic', b'data', QOS_0, retain=True)
                await pub_client.disconnect()
                await asyncio.sleep(0.1)
                self.assertIn('/topic', broker._retained_messages)
                retained_message = broker._retained_messages['/topic']
                self.assertEquals(retained_message.source_session, pub_client.session)
                self.assertEquals(retained_message.topic, '/topic')
                self.assertEquals(retained_message.data, b'data')
                self.assertEquals(retained_message.qos, QOS_0)
                await broker.shutdown()
                self.assertTrue(broker.transitions.is_stopped())
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()

    @patch('hbmqtt.broker.PluginManager', new_callable=AsyncMock)
    def test_client_subscribe_publish(self, MockPluginManager):
        async def test_coro():
            try:
                broker = Broker(test_config, plugin_namespace="hbmqtt.test.plugins")
                await broker.start()
                self.assertTrue(broker.transitions.is_started())
                sub_client = MQTTClient()
                await sub_client.connect('mqtt://localhost')
                ret = await sub_client.subscribe([('/qos0', QOS_0), ('/qos1', QOS_1), ('/qos2', QOS_2)])
                self.assertEquals(ret, [QOS_0, QOS_1, QOS_2])

                await self._client_publish('/qos0', b'data', QOS_0)
                await self._client_publish('/qos1', b'data', QOS_1)
                await self._client_publish('/qos2', b'data', QOS_2)
                await asyncio.sleep(0.1)
                for qos in [QOS_0, QOS_1, QOS_2]:
                    message = await sub_client.deliver_message()
                    self.assertIsNotNone(message)
                    self.assertEquals(message.topic, '/qos%s' % qos)
                    self.assertEquals(message.data, b'data')
                    self.assertEquals(message.qos, qos)
                await sub_client.disconnect()
                await asyncio.sleep(0.1)
                await broker.shutdown()
                self.assertTrue(broker.transitions.is_stopped())
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()

    @patch('hbmqtt.broker.PluginManager', new_callable=AsyncMock)
    def test_client_publish_retain_subscribe(self, MockPluginManager):
        async def test_coro():
            try:
                broker = Broker(test_config, plugin_namespace="hbmqtt.test.plugins")
                await broker.start()
                self.assertTrue(broker.transitions.is_started())
                sub_client = MQTTClient()
                await sub_client.connect('mqtt://localhost', cleansession=False)
                ret = await sub_client.subscribe([('/qos0', QOS_0), ('/qos1', QOS_1), ('/qos2', QOS_2)])
                self.assertEquals(ret, [QOS_0, QOS_1, QOS_2])
                await sub_client.disconnect()
                await asyncio.sleep(0.1)

                await self._client_publish('/qos0', b'data', QOS_0, retain=True)
                await self._client_publish('/qos1', b'data', QOS_1, retain=True)
                await self._client_publish('/qos2', b'data', QOS_2, retain=True)
                await sub_client.reconnect()
                for qos in [QOS_0, QOS_1, QOS_2]:
                    log.debug("TEST QOS: %d" % qos)
                    message = await sub_client.deliver_message()
                    log.debug("Message: " + repr(message.publish_packet))
                    self.assertIsNotNone(message)
                    self.assertEquals(message.topic, '/qos%s' % qos)
                    self.assertEquals(message.data, b'data')
                    self.assertEquals(message.qos, qos)
                await sub_client.disconnect()
                await asyncio.sleep(0.1)
                await broker.shutdown()
                self.assertTrue(broker.transitions.is_stopped())
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()

    async def _client_publish(self, topic, data, qos, retain=False):
        pub_client = MQTTClient()
        ret = await pub_client.connect('mqtt://localhost/')
        self.assertEqual(ret, 0)
        ret = await pub_client.publish(topic, data, qos, retain)
        await pub_client.disconnect()
        return ret
