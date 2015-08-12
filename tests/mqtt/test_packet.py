# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio

from hbmqtt.mqtt.packet import CONNECT, MQTTFixedHeader
from hbmqtt.errors import MQTTException
from hbmqtt.adapters import BufferReader


class TestMQTTFixedHeaderTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_from_bytes(self):
        data = b'\x10\x7f'
        stream = BufferReader(data)
        header = self.loop.run_until_complete(MQTTFixedHeader.from_stream(stream))
        self.assertEqual(header.packet_type, CONNECT)
        self.assertFalse(header.flags & 0x08)
        self.assertEqual((header.flags & 0x06) >> 1, 0)
        self.assertFalse(header.flags & 0x01)
        self.assertEqual(header.remaining_length, 127)

    def test_from_bytes_with_length(self):
        data = b'\x10\xff\xff\xff\x7f'
        stream = BufferReader(data)
        header = self.loop.run_until_complete(MQTTFixedHeader.from_stream(stream))
        self.assertEqual(header.packet_type, CONNECT)
        self.assertFalse(header.flags & 0x08)
        self.assertEqual((header.flags & 0x06) >> 1, 0)
        self.assertFalse(header.flags & 0x01)
        self.assertEqual(header.remaining_length, 268435455)

    def test_from_bytes_ko_with_length(self):
        data = b'\x10\xff\xff\xff\xff\x7f'
        stream = BufferReader(data)
        with self.assertRaises(MQTTException):
            self.loop.run_until_complete(MQTTFixedHeader.from_stream(stream))

    def test_to_bytes(self):
        header = MQTTFixedHeader(CONNECT, 0x00, 0)
        data = header.to_bytes()
        self.assertEqual(data, b'\x10\x00')

    def test_to_bytes_2(self):
        header = MQTTFixedHeader(CONNECT, 0x00, 268435455)
        data = header.to_bytes()
        self.assertEqual(data, b'\x10\xff\xff\xff\x7f')
