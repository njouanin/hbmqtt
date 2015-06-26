# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio

from hbmqtt.mqtt.connect import ConnectPacket, ConnectVariableHeader, ConnectPayload
from hbmqtt.session import Session
from hbmqtt.mqtt.packet import PacketType
import logging

logging.basicConfig(level=logging.DEBUG)

ret_packet = None

class ConnectPacketTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.logger = logging.getLogger(__name__)

    def test_read_loop(self):
        data = b'\x10\x3e\x00\x04MQTT\x04\xce\x00\x00\x00\x0a0123456789\x00\x09WillTopic\x00\x0bWillMessage\x00\x04user\x00\x08password'
        @asyncio.coroutine
        def serve_test(reader, writer):
            writer.write(data)
            yield from writer.drain()
            writer.close()

        loop = asyncio.get_event_loop()
        coro = asyncio.start_server(serve_test, '127.0.0.1', 8888, loop=loop)
        server = loop.run_until_complete(coro)

        @asyncio.coroutine
        def client():
            S = Session(loop)
            S.reader, S.writer = yield from asyncio.open_connection('127.0.0.1', 8888,
                                                        loop=loop)
            yield from S.start()
            incoming_packet = yield from S.incoming_queues[PacketType.CONNECT].get()
            S.writer.close()
            yield from S.stop()
            return incoming_packet

        packet = loop.run_until_complete(client())
        server.close()
        loop.stop()
        self.assertEquals(packet.fixed_header.packet_type, PacketType.CONNECT)

    def test_write_loop(self):
        test_packet = ConnectPacket(vh=ConnectVariableHeader(), payload=ConnectPayload('Id', 'WillTopic', 'WillMessage', 'user', 'password'))
        event=asyncio.Event()

        @asyncio.coroutine
        def serve_test(reader, writer):
            global ret_packet
            packet = yield from ConnectPacket.from_stream(reader)
            ret_packet = packet
            writer.close()
            event.set()

        @asyncio.coroutine
        def client():
            S = Session(loop)
            S.reader, S.writer = yield from asyncio.open_connection('127.0.0.1', 8888, loop=loop)
            yield from S.start()
            yield from S.outgoing_queue.put(test_packet)
            yield from S.stop()
            S.writer.close()

        # Start server
        loop = asyncio.get_event_loop()
        coro = asyncio.start_server(serve_test, '127.0.0.1', 8888, loop=loop)
        server = loop.run_until_complete(coro)

        # Schedule client
        loop.call_soon(asyncio.async, client())

        # Wait for server to complete client request
        loop.run_until_complete(asyncio.wait([event.wait()]))
        server.close()
        self.logger.info(ret_packet)
        self.assertEquals(ret_packet.fixed_header.packet_type, PacketType.CONNECT)