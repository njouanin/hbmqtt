# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio

from hbmqtt.messages.packet import PacketType, MQTTPacket, MQTTFixedHeader
from hbmqtt.errors import MQTTException


class TestMQTTFixedHeaderTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_from_bytes(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\x10\x7f')
        header = self.loop.run_until_complete(MQTTFixedHeader.from_stream(stream))
        self.assertEqual(header.packet_type, PacketType.CONNECT)
        self.assertFalse(header.flags & 0x08)
        self.assertEqual((header.flags & 0x06) >> 1, 0)
        self.assertFalse(header.flags & 0x01)
        self.assertEqual(header.remaining_length, 127)

    def test_from_bytes_with_length(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\x10\xff\xff\xff\x7f')
        header = self.loop.run_until_complete(MQTTFixedHeader.from_stream(stream))
        self.assertEqual(header.packet_type, PacketType.CONNECT)
        self.assertFalse(header.flags & 0x08)
        self.assertEqual((header.flags & 0x06) >> 1, 0)
        self.assertFalse(header.flags & 0x01)
        self.assertEqual(header.remaining_length, 268435455)

    def test_from_bytes_reserved(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\x0f\x7f')
        with self.assertRaises(MQTTException):
            self.loop.run_until_complete(MQTTFixedHeader.from_stream(stream))

    def test_from_bytes_ko_with_length(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\x10\xff\xff\xff\xff\x7f')
        with self.assertRaises(MQTTException):
            self.loop.run_until_complete(MQTTFixedHeader.from_stream(stream))

    def test_to_bytes(self):
        header = MQTTFixedHeader(PacketType.CONNECT, 0x00, 0)
        data = header.to_bytes()
        self.assertEqual(data, b'\x10\x00')

    def test_to_bytes_2(self):
        header = MQTTFixedHeader(PacketType.CONNECT, 0x00, 268435455)
        data = header.to_bytes()
        self.assertEqual(data, b'\x10\xff\xff\xff\x7f')
