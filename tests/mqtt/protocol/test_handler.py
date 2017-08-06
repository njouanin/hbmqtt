# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio
import logging
import random
from hbmqtt.plugins.manager import PluginManager
from hbmqtt.session import Session, OutgoingApplicationMessage, IncomingApplicationMessage
from hbmqtt.mqtt.protocol.handler import ProtocolHandler
from hbmqtt.adapters import StreamWriterAdapter, StreamReaderAdapter
from hbmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2
from hbmqtt.mqtt.publish import PublishPacket
from hbmqtt.mqtt.puback import PubackPacket
from hbmqtt.mqtt.pubrec import PubrecPacket
from hbmqtt.mqtt.pubrel import PubrelPacket
from hbmqtt.mqtt.pubcomp import PubcompPacket

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=formatter)
log = logging.getLogger(__name__)


def rand_packet_id():
    return random.randint(0, 65535)


def adapt(reader, writer):
    return StreamReaderAdapter(reader), StreamWriterAdapter(writer)


class ProtocolHandlerTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.plugin_manager = PluginManager("hbmqtt.test.plugins", context=None, loop=self.loop)

    def tearDown(self):
        self.loop.close()

    def test_init_handler(self):
        Session()
        handler = ProtocolHandler(self.plugin_manager, loop=self.loop)
        self.assertIsNone(handler.session)
        self.assertIs(handler._loop, self.loop)
        self.check_empty_waiters(handler)

    def test_start_stop(self):
        @asyncio.coroutine
        def server_mock(reader, writer):
            pass

        @asyncio.coroutine
        def test_coro():
            try:
                s = Session()
                reader, writer = yield from asyncio.open_connection('127.0.0.1', 8888)
                reader_adapted, writer_adapted = adapt(reader, writer)
                handler = ProtocolHandler(self.plugin_manager)
                handler.attach(s, reader_adapted, writer_adapted)
                yield from self.start_handler(handler, s)
                yield from self.stop_handler(handler, s)
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        coro = asyncio.start_server(server_mock, '127.0.0.1', 8888)
        server = self.loop.run_until_complete(coro)
        self.loop.run_until_complete(test_coro())
        server.close()
        self.loop.run_until_complete(server.wait_closed())
        if future.exception():
            raise future.exception()

    def test_publish_qos0(self):
        @asyncio.coroutine
        def server_mock(reader, writer):
            try:
                packet = yield from PublishPacket.from_stream(reader)
                self.assertEqual(packet.variable_header.topic_name, '/topic')
                self.assertEqual(packet.qos, QOS_0)
                self.assertIsNone(packet.packet_id)
            except Exception as ae:
                future.set_exception(ae)

        @asyncio.coroutine
        def test_coro():
            try:
                s = Session()
                reader, writer = yield from asyncio.open_connection('127.0.0.1', 8888, loop=self.loop)
                reader_adapted, writer_adapted = adapt(reader, writer)
                handler = ProtocolHandler(self.plugin_manager, loop=self.loop)
                handler.attach(s, reader_adapted, writer_adapted)
                yield from self.start_handler(handler, s)
                message = yield from handler.mqtt_publish('/topic', b'test_data', QOS_0, False)
                self.assertIsInstance(message, OutgoingApplicationMessage)
                self.assertIsNotNone(message.publish_packet)
                self.assertIsNone(message.puback_packet)
                self.assertIsNone(message.pubrec_packet)
                self.assertIsNone(message.pubrel_packet)
                self.assertIsNone(message.pubcomp_packet)
                yield from self.stop_handler(handler, s)
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        future = asyncio.Future(loop=self.loop)
        coro = asyncio.start_server(server_mock, '127.0.0.1', 8888, loop=self.loop)
        server = self.loop.run_until_complete(coro)
        self.loop.run_until_complete(test_coro())
        server.close()
        self.loop.run_until_complete(server.wait_closed())
        if future.exception():
            raise future.exception()

    def test_publish_qos1(self):
        @asyncio.coroutine
        def server_mock(reader, writer):
            packet = yield from PublishPacket.from_stream(reader)
            try:
                self.assertEqual(packet.variable_header.topic_name, '/topic')
                self.assertEqual(packet.qos, QOS_1)
                self.assertIsNotNone(packet.packet_id)
                self.assertIn(packet.packet_id, self.session.inflight_out)
                self.assertIn(packet.packet_id, self.handler._puback_waiters)
                puback = PubackPacket.build(packet.packet_id)
                yield from puback.to_stream(writer)
            except Exception as ae:
                future.set_exception(ae)

        @asyncio.coroutine
        def test_coro():
            try:
                reader, writer = yield from asyncio.open_connection('127.0.0.1', 8888, loop=self.loop)
                reader_adapted, writer_adapted = adapt(reader, writer)
                self.handler = ProtocolHandler(self.plugin_manager, loop=self.loop)
                self.handler.attach(self.session, reader_adapted, writer_adapted)
                yield from self.start_handler(self.handler, self.session)
                message = yield from self.handler.mqtt_publish('/topic', b'test_data', QOS_1, False)
                self.assertIsInstance(message, OutgoingApplicationMessage)
                self.assertIsNotNone(message.publish_packet)
                self.assertIsNotNone(message.puback_packet)
                self.assertIsNone(message.pubrec_packet)
                self.assertIsNone(message.pubrel_packet)
                self.assertIsNone(message.pubcomp_packet)
                yield from self.stop_handler(self.handler, self.session)
                if not future.done():
                    future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)
        self.handler = None
        self.session = Session()
        future = asyncio.Future(loop=self.loop)

        coro = asyncio.start_server(server_mock, '127.0.0.1', 8888, loop=self.loop)
        server = self.loop.run_until_complete(coro)
        self.loop.run_until_complete(test_coro())
        server.close()
        self.loop.run_until_complete(server.wait_closed())
        if future.exception():
            raise future.exception()

    def test_publish_qos2(self):
        @asyncio.coroutine
        def server_mock(reader, writer):
            try:
                packet = yield from PublishPacket.from_stream(reader)
                self.assertEqual(packet.topic_name, '/topic')
                self.assertEqual(packet.qos, QOS_2)
                self.assertIsNotNone(packet.packet_id)
                self.assertIn(packet.packet_id, self.session.inflight_out)
                self.assertIn(packet.packet_id, self.handler._pubrec_waiters)
                pubrec = PubrecPacket.build(packet.packet_id)
                yield from pubrec.to_stream(writer)

                yield from PubrelPacket.from_stream(reader)
                self.assertIn(packet.packet_id, self.handler._pubcomp_waiters)
                pubcomp = PubcompPacket.build(packet.packet_id)
                yield from pubcomp.to_stream(writer)
            except Exception as ae:
                future.set_exception(ae)

        @asyncio.coroutine
        def test_coro():
            try:
                reader, writer = yield from asyncio.open_connection('127.0.0.1', 8888, loop=self.loop)
                reader_adapted, writer_adapted = adapt(reader, writer)
                self.handler = ProtocolHandler(self.plugin_manager, loop=self.loop)
                self.handler.attach(self.session, reader_adapted, writer_adapted)
                yield from self.start_handler(self.handler, self.session)
                message = yield from self.handler.mqtt_publish('/topic', b'test_data', QOS_2, False)
                self.assertIsInstance(message, OutgoingApplicationMessage)
                self.assertIsNotNone(message.publish_packet)
                self.assertIsNone(message.puback_packet)
                self.assertIsNotNone(message.pubrec_packet)
                self.assertIsNotNone(message.pubrel_packet)
                self.assertIsNotNone(message.pubcomp_packet)
                yield from self.stop_handler(self.handler, self.session)
                if not future.done():
                    future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)
        self.handler = None
        self.session = Session()
        future = asyncio.Future(loop=self.loop)

        coro = asyncio.start_server(server_mock, '127.0.0.1', 8888, loop=self.loop)
        server = self.loop.run_until_complete(coro)
        self.loop.run_until_complete(test_coro())
        server.close()
        self.loop.run_until_complete(server.wait_closed())
        if future.exception():
            raise future.exception()

    def test_receive_qos0(self):
        @asyncio.coroutine
        def server_mock(reader, writer):
            packet = PublishPacket.build('/topic', b'test_data', rand_packet_id(), False, QOS_0, False)
            yield from packet.to_stream(writer)

        @asyncio.coroutine
        def test_coro():
            try:
                reader, writer = yield from asyncio.open_connection('127.0.0.1', 8888, loop=self.loop)
                reader_adapted, writer_adapted = adapt(reader, writer)
                self.handler = ProtocolHandler(self.plugin_manager, loop=self.loop)
                self.handler.attach(self.session, reader_adapted, writer_adapted)
                yield from self.start_handler(self.handler, self.session)
                message = yield from self.handler.mqtt_deliver_next_message()
                self.assertIsInstance(message, IncomingApplicationMessage)
                self.assertIsNotNone(message.publish_packet)
                self.assertIsNone(message.puback_packet)
                self.assertIsNone(message.pubrec_packet)
                self.assertIsNone(message.pubrel_packet)
                self.assertIsNone(message.pubcomp_packet)
                yield from self.stop_handler(self.handler, self.session)
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        self.handler = None
        self.session = Session()
        future = asyncio.Future(loop=self.loop)
        coro = asyncio.start_server(server_mock, '127.0.0.1', 8888, loop=self.loop)
        server = self.loop.run_until_complete(coro)
        self.loop.run_until_complete(test_coro())
        server.close()
        self.loop.run_until_complete(server.wait_closed())
        if future.exception():
            raise future.exception()

    def test_receive_qos1(self):
        @asyncio.coroutine
        def server_mock(reader, writer):
            try:
                packet = PublishPacket.build('/topic', b'test_data', rand_packet_id(), False, QOS_1, False)
                yield from packet.to_stream(writer)
                puback = yield from PubackPacket.from_stream(reader)
                self.assertIsNotNone(puback)
                self.assertEqual(packet.packet_id, puback.packet_id)
            except Exception as ae:
                print(ae)
                future.set_exception(ae)

        @asyncio.coroutine
        def test_coro():
            try:
                reader, writer = yield from asyncio.open_connection('127.0.0.1', 8888, loop=self.loop)
                reader_adapted, writer_adapted = adapt(reader, writer)
                self.handler = ProtocolHandler(self.plugin_manager, loop=self.loop)
                self.handler.attach(self.session, reader_adapted, writer_adapted)
                yield from self.start_handler(self.handler, self.session)
                message = yield from self.handler.mqtt_deliver_next_message()
                self.assertIsInstance(message, IncomingApplicationMessage)
                self.assertIsNotNone(message.publish_packet)
                self.assertIsNotNone(message.puback_packet)
                self.assertIsNone(message.pubrec_packet)
                self.assertIsNone(message.pubrel_packet)
                self.assertIsNone(message.pubcomp_packet)
                yield from self.stop_handler(self.handler, self.session)
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        self.handler = None
        self.session = Session()
        future = asyncio.Future(loop=self.loop)
        self.event = asyncio.Event(loop=self.loop)
        coro = asyncio.start_server(server_mock, '127.0.0.1', 8888, loop=self.loop)
        server = self.loop.run_until_complete(coro)
        self.loop.run_until_complete(test_coro())
        server.close()
        self.loop.run_until_complete(server.wait_closed())
        if future.exception():
            raise future.exception()

    def test_receive_qos2(self):
        @asyncio.coroutine
        def server_mock(reader, writer):
            try:
                packet = PublishPacket.build('/topic', b'test_data', rand_packet_id(), False, QOS_2, False)
                yield from packet.to_stream(writer)
                pubrec = yield from PubrecPacket.from_stream(reader)
                self.assertIsNotNone(pubrec)
                self.assertEqual(packet.packet_id, pubrec.packet_id)
                self.assertIn(packet.packet_id, self.handler._pubrel_waiters)
                pubrel = PubrelPacket.build(packet.packet_id)
                yield from pubrel.to_stream(writer)
                pubcomp = yield from PubcompPacket.from_stream(reader)
                self.assertIsNotNone(pubcomp)
                self.assertEqual(packet.packet_id, pubcomp.packet_id)
            except Exception as ae:
                future.set_exception(ae)

        @asyncio.coroutine
        def test_coro():
            try:
                reader, writer = yield from asyncio.open_connection('127.0.0.1', 8888, loop=self.loop)
                reader_adapted, writer_adapted = adapt(reader, writer)
                self.handler = ProtocolHandler(self.plugin_manager, loop=self.loop)
                self.handler.attach(self.session, reader_adapted, writer_adapted)
                yield from self.start_handler(self.handler, self.session)
                message = yield from self.handler.mqtt_deliver_next_message()
                self.assertIsInstance(message, IncomingApplicationMessage)
                self.assertIsNotNone(message.publish_packet)
                self.assertIsNone(message.puback_packet)
                self.assertIsNotNone(message.pubrec_packet)
                self.assertIsNotNone(message.pubrel_packet)
                self.assertIsNotNone(message.pubcomp_packet)
                yield from self.stop_handler(self.handler, self.session)
                future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)

        self.handler = None
        self.session = Session()
        future = asyncio.Future(loop=self.loop)
        coro = asyncio.start_server(server_mock, '127.0.0.1', 8888, loop=self.loop)
        server = self.loop.run_until_complete(coro)
        self.loop.run_until_complete(test_coro())
        server.close()
        self.loop.run_until_complete(server.wait_closed())
        if future.exception():
            raise future.exception()

    @asyncio.coroutine
    def start_handler(self, handler, session):
        self.check_empty_waiters(handler)
        self.check_no_message(session)
        yield from handler.start()
        self.assertTrue(handler._reader_ready)

    @asyncio.coroutine
    def stop_handler(self, handler, session):
        yield from handler.stop()
        self.assertTrue(handler._reader_stopped)
        self.check_empty_waiters(handler)
        self.check_no_message(session)

    def check_empty_waiters(self, handler):
        self.assertFalse(handler._puback_waiters)
        self.assertFalse(handler._pubrec_waiters)
        self.assertFalse(handler._pubrel_waiters)
        self.assertFalse(handler._pubcomp_waiters)

    def check_no_message(self, session):
        self.assertFalse(session.inflight_out)
        self.assertFalse(session.inflight_in)

    def test_publish_qos1_retry(self):
        @asyncio.coroutine
        def server_mock(reader, writer):
            packet = yield from PublishPacket.from_stream(reader)
            try:
                self.assertEqual(packet.topic_name, '/topic')
                self.assertEqual(packet.qos, QOS_1)
                self.assertIsNotNone(packet.packet_id)
                self.assertIn(packet.packet_id, self.session.inflight_out)
                self.assertIn(packet.packet_id, self.handler._puback_waiters)
                puback = PubackPacket.build(packet.packet_id)
                yield from puback.to_stream(writer)
            except Exception as ae:
                future.set_exception(ae)

        @asyncio.coroutine
        def test_coro():
            try:
                reader, writer = yield from asyncio.open_connection('127.0.0.1', 8888, loop=self.loop)
                reader_adapted, writer_adapted = adapt(reader, writer)
                self.handler = ProtocolHandler(self.plugin_manager, loop=self.loop)
                self.handler.attach(self.session, reader_adapted, writer_adapted)
                yield from self.handler.start()
                yield from self.stop_handler(self.handler, self.session)
                if not future.done():
                    future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)
        self.handler = None
        self.session = Session()
        message = OutgoingApplicationMessage(1, '/topic', QOS_1, b'test_data', False)
        message.publish_packet = PublishPacket.build('/topic', b'test_data', rand_packet_id(), False, QOS_1, False)
        self.session.inflight_out[1] = message
        future = asyncio.Future(loop=self.loop)

        coro = asyncio.start_server(server_mock, '127.0.0.1', 8888, loop=self.loop)
        server = self.loop.run_until_complete(coro)
        self.loop.run_until_complete(test_coro())
        server.close()
        self.loop.run_until_complete(server.wait_closed())
        if future.exception():
            raise future.exception()

    def test_publish_qos2_retry(self):
        @asyncio.coroutine
        def server_mock(reader, writer):
            try:
                packet = yield from PublishPacket.from_stream(reader)
                self.assertEqual(packet.topic_name, '/topic')
                self.assertEqual(packet.qos, QOS_2)
                self.assertIsNotNone(packet.packet_id)
                self.assertIn(packet.packet_id, self.session.inflight_out)
                self.assertIn(packet.packet_id, self.handler._pubrec_waiters)
                pubrec = PubrecPacket.build(packet.packet_id)
                yield from pubrec.to_stream(writer)

                yield from PubrelPacket.from_stream(reader)
                self.assertIn(packet.packet_id, self.handler._pubcomp_waiters)
                pubcomp = PubcompPacket.build(packet.packet_id)
                yield from pubcomp.to_stream(writer)
            except Exception as ae:
                future.set_exception(ae)

        @asyncio.coroutine
        def test_coro():
            try:
                reader, writer = yield from asyncio.open_connection('127.0.0.1', 8888, loop=self.loop)
                reader_adapted, writer_adapted = adapt(reader, writer)
                self.handler = ProtocolHandler(self.plugin_manager, loop=self.loop)
                self.handler.attach(self.session, reader_adapted, writer_adapted)
                yield from self.handler.start()
                yield from self.stop_handler(self.handler, self.session)
                if not future.done():
                    future.set_result(True)
            except Exception as ae:
                future.set_exception(ae)
        self.handler = None
        self.session = Session()
        message = OutgoingApplicationMessage(1, '/topic', QOS_2, b'test_data', False)
        message.publish_packet = PublishPacket.build('/topic', b'test_data', rand_packet_id(), False, QOS_2, False)
        self.session.inflight_out[1] = message
        future = asyncio.Future(loop=self.loop)

        coro = asyncio.start_server(server_mock, '127.0.0.1', 8888, loop=self.loop)
        server = self.loop.run_until_complete(coro)
        self.loop.run_until_complete(test_coro())
        server.close()
        self.loop.run_until_complete(server.wait_closed())
        if future.exception():
            raise future.exception()
