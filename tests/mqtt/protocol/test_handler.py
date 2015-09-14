# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio
import logging
from hbmqtt.plugins.manager import PluginManager
from hbmqtt.session import Session
from hbmqtt.mqtt.protocol.handler import ProtocolHandler
from hbmqtt.adapters import StreamWriterAdapter, StreamReaderAdapter
from hbmqtt.mqtt.constants import *
from hbmqtt.mqtt.publish import PublishPacket

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=formatter)
log = logging.getLogger(__name__)


def adapt(reader, writer):
    return StreamReaderAdapter(reader), StreamWriterAdapter(writer)


class ProtocolHandlerTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.plugin_manager = PluginManager("hbmqtt.test.plugins", context=None, loop=self.loop)

    def tearDown(self):
        self.loop.close()

    def test_init_handler(self):
        s = Session()
        handler = ProtocolHandler(s, self.plugin_manager, loop=self.loop)
        self.assertIs(handler.session, s)
        self.assertIs(handler._loop, self.loop)
        self.check_empty_waiters(handler)

    def test_start_stop(self):
        @asyncio.coroutine
        def server_mock(reader, writer):
            pass

        @asyncio.coroutine
        def test_coro():
            s = Session()
            reader, writer = yield from asyncio.open_connection('127.0.0.1', 8888, loop=self.loop)
            s.reader, s.writer = adapt(reader, writer)
            handler = ProtocolHandler(s, self.plugin_manager, loop=self.loop)
            yield from self.start_handler(handler, s, future)
            yield from self.stop_handler(handler, s, future)
            future.set_result(True)

        future = asyncio.Future(loop=self.loop)
        coro = asyncio.start_server(server_mock, '127.0.0.1', 8888, loop=self.loop)
        server = self.loop.run_until_complete(coro)
        self.loop.run_until_complete(test_coro())
        server.close()
        self.loop.run_until_complete(server.wait_closed())
        if future.exception():
            raise future.exception()

    def test_publish_qos0(self):
        @asyncio.coroutine
        def server_mock(reader, writer):
            packet = yield from PublishPacket.from_stream(reader)
            try:
                self.assertEquals(packet.topic_name, '/topic')
                self.assertEquals(packet.qos, QOS_0)
                self.assertIsNone(packet.packet_id)
            except AssertionError as ae:
                future.set_exception(ae)

        @asyncio.coroutine
        def test_coro():
            s = Session()
            reader, writer = yield from asyncio.open_connection('127.0.0.1', 8888, loop=self.loop)
            s.reader, s.writer = adapt(reader, writer)
            handler = ProtocolHandler(s, self.plugin_manager, loop=self.loop)
            yield from self.start_handler(handler, s, future)
            message = yield from handler.mqtt_publish('/topic', b'test_data', QOS_0, False)
            yield from self.stop_handler(handler, s, future)
            future.set_result(True)

        future = asyncio.Future(loop=self.loop)
        coro = asyncio.start_server(server_mock, '127.0.0.1', 8888, loop=self.loop)
        server = self.loop.run_until_complete(coro)
        self.loop.run_until_complete(test_coro())
        server.close()
        self.loop.run_until_complete(server.wait_closed())
        if future.exception():
            raise future.exception()


    @asyncio.coroutine
    def start_handler(self, handler, session, future):
        yield from handler.start()
        try:
            self.assertTrue(handler._reader_ready)
            self.check_empty_waiters(handler)
            self.check_no_message(session, future)
        except AssertionError as ae:
            if future and not future.cancelled():
                future.set_exception(ae)
            else:
                raise ae

    @asyncio.coroutine
    def stop_handler(self, handler, session, future):
        yield from handler.stop()
        try:
            self.assertTrue(handler._reader_stopped)
            self.check_empty_waiters(handler)
            self.check_no_message(session, future)
        except AssertionError as ae:
            if future and not future.cancelled():
                future.set_exception(ae)
            else:
                raise ae

    def check_empty_waiters(self, handler, future=None):
        try:
            self.assertFalse(handler._puback_waiters)
            self.assertFalse(handler._pubrec_waiters)
            self.assertFalse(handler._pubrel_waiters)
            self.assertFalse(handler._pubcomp_waiters)
        except AssertionError as ae:
            if future and not future.cancelled():
                future.set_exception(ae)
            else:
                raise ae

    def check_no_message(self, session, future):
        try:
            self.assertFalse(session.inflight_out)
            self.assertFalse(session.inflight_in)
            self.assertEquals(session.delivered_message_queue.qsize(), 0)
        except AssertionError as ae:
            if future and not future.cancelled():
                future.set_exception(ae)
            else:
                raise ae
