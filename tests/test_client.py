# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio
import os
import logging
from hbmqtt.client import MQTTClient
from hbmqtt.mqtt.constants import *

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=formatter)
log = logging.getLogger(__name__)


class MQTTClientTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

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

    def test_subscribe(self):
        @asyncio.coroutine
        def test_coro():
            try:
                client = MQTTClient()
                yield from client.connect('mqtt://test.mosquitto.org/')
                self.assertIsNotNone(client.session)
                ret = yield from client.subscribe([
                    ('$SYS/broker/uptime', QOS_0),
                    ('$SYS/broker/uptime', QOS_1),
                    ('$SYS/broker/uptime', QOS_2),
                ])
                self.assertEquals(ret[0], QOS_0)
                self.assertEquals(ret[1], QOS_1)
                self.assertEquals(ret[2], QOS_2)
                yield from client.disconnect()
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()

    def test_unsubscribe(self):
        @asyncio.coroutine
        def test_coro():
            try:
                client = MQTTClient()
                yield from client.connect('mqtt://test.mosquitto.org/')
                self.assertIsNotNone(client.session)
                ret = yield from client.subscribe([
                    ('$SYS/broker/uptime', QOS_0),
                ])
                self.assertEquals(ret[0], QOS_0)
                yield from client.unsubscribe(['$SYS/broker/uptime'])
                yield from client.disconnect()
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        self.loop.run_until_complete(test_coro())
        if future.exception():
            raise future.exception()
