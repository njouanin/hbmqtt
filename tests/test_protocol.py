# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio

from hbmqtt.mqtt.connect import ConnectPacket, ConnectVariableHeader, ConnectPayload
from hbmqtt.mqtt.packet import MQTTFixedHeader, PacketType
from hbmqtt.errors import MQTTException
from hbmqtt.session import Session
from hbmqtt.protocol import ProtocolHandler
from hbmqtt.mqtt.packet import PacketType
import logging

logging.basicConfig(level=logging.DEBUG)

class ConnectPacketTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

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
            S = Session()
            S.reader, S.writer = yield from asyncio.open_connection('127.0.0.1', 8888,
                                                        loop=loop)
            handler = ProtocolHandler(S, loop)
            handler.start()
            incoming_packet = yield from S.incoming_queues[PacketType.CONNECT].get()
            handler.stop()
            return incoming_packet

        packet = loop.run_until_complete(client())
        server.close()
        self.assertEquals(packet.fixed_header.packet_type, PacketType.CONNECT)