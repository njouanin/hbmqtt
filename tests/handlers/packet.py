# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio

from hbmqtt.handlers.packet import PacketHandler
from hbmqtt.messages.packet import PacketType, MQTTHeader
from hbmqtt.errors import MQTTException


class TestPacketHandler(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_read_packet_header(self):
        packet_handler = PacketHandler()
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\x10\x7f')
        header = self.loop.run_until_complete(packet_handler.read_packet_header(stream))
        self.assertEqual(header.message_type, PacketType.CONNECT)
        self.assertFalse(header.flags & 0x08)
        self.assertEqual((header.flags & 0x06) >> 1, 0)
        self.assertFalse(header.flags & 0x01)
        self.assertEqual(header.remaining_length, 127)

    def test_decode_ok_with_length(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\x10\xff\xff\xff\x7f')
        header = self.loop.run_until_complete(PacketHandler.read_packet_header(stream))
        self.assertEqual(header.packet_type, PacketType.CONNECT)
        self.assertFalse(header.flags & 0x08)
        self.assertEqual((header.flags & 0x06) >> 1, 0)
        self.assertFalse(header.flags & 0x01)
        self.assertEqual(header.remaining_length, 268435455)

    def test_decode_reserved(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\x0f\x7f')
        with self.assertRaises(MQTTException):
            self.loop.run_until_complete(PacketHandler.read_packet_header(stream))

    def test_decode_ko_with_length(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\x10\xff\xff\xff\xff\x7f')
        with self.assertRaises(MQTTException):
            self.loop.run_until_complete(PacketHandler.read_packet_header(stream))

    def test_encode(self):
        header = MQTTHeader(PacketType.CONNECT, 0x00, 0)
        data = self.loop.run_until_complete(PacketHandler._encode_fixed_header(header))
        self.assertEqual(data, b'\x10\x00')

    def test_encode_2(self):
        header = MQTTHeader(PacketType.CONNECT, 0x00, 268435455)
        data = self.loop.run_until_complete(PacketHandler._encode_fixed_header(header))
        self.assertEqual(data, b'\x10\xff\xff\xff\x7f')
