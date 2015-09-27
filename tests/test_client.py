# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio
import os
import logging
from hbmqtt.plugins.manager import PluginManager
from hbmqtt.client import MQTTClient

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=formatter)
log = logging.getLogger(__name__)


class MQTTClientTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.plugin_manager = PluginManager("hbmqtt.test.plugins", context=None, loop=self.loop)

    def tearDown(self):
        self.loop.close()

    def test_connect_tcp(self):
        @asyncio.coroutine
        def test_coro():
            try:
                client = MQTTClient()
                ret = yield from client.connect('mqtt://test.mosquitto.org/')
                self.assertIsNotNone(client.session)
                yield from client.disconnect()
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()

    def test_connect_tcp_secure(self):
        @asyncio.coroutine
        def test_coro():
            try:
                client = MQTTClient()
                print(os.path.dirname(os.path.realpath(__file__)))
                ca = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mosquitto.org.crt')
                ret = yield from client.connect('mqtts://test.mosquitto.org/', cafile=ca)
                self.assertIsNotNone(client.session)
                yield from client.disconnect()
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()

    def test_connect_tcp_failure(self):
        @asyncio.coroutine
        def test_coro():
            try:
                client = MQTTClient()
                ret = yield from client.connect('mqtt://localhost/')
                print(ret)
            except Exception as e:
                future.set_result(True)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()

    def test_connect_ws(self):
        @asyncio.coroutine
        def test_coro():
            try:
                client = MQTTClient()
                yield from client.connect('ws://test.mosquitto.org:8080/')
                self.assertIsNotNone(client.session)
                yield from client.disconnect()
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()

    def test_connect_ws_secure(self):
        @asyncio.coroutine
        def test_coro():
            try:
                client = MQTTClient()
                ca = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mosquitto.org.crt')
                yield from client.connect('wss://test.mosquitto.org:8081/', cafile=ca)
                self.assertIsNotNone(client.session)
                yield from client.disconnect()
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()

    def test_ping(self):
        @asyncio.coroutine
        def test_coro():
            try:
                client = MQTTClient()
                ret = yield from client.connect('mqtt://test.mosquitto.org/')
                self.assertIsNotNone(client.session)
                yield from client.ping()
                yield from client.disconnect()
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()
