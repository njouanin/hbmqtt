# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio
from hbmqtt.codecs.header import MQTTHeaderCodec, MQTTHeaderException
from hbmqtt.message import MessageType

class TestMQTTHeaderCodec(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_decode_ok(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\x10\x7f')
        header = self.loop.run_until_complete(MQTTHeaderCodec.decode(stream))
        self.assertEqual(header.message_type, MessageType.CONNECT)
        self.assertFalse(header.flags & 0x08)
        self.assertEqual((header.flags & 0x06) >> 1, 0)
        self.assertFalse(header.flags & 0x01)
        self.assertEqual(header.remaining_length, 127)

    def test_decode_ok_with_length(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\x10\xff\xff\xff\x7f')
        header = self.loop.run_until_complete(MQTTHeaderCodec.decode(stream))
        self.assertEqual(header.message_type, MessageType.CONNECT)
        self.assertFalse(header.flags & 0x08)
        self.assertEqual((header.flags & 0x06) >> 1, 0)
        self.assertFalse(header.flags & 0x01)
        self.assertEqual(header.remaining_length, 268435455)

    def test_decode_reserved(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\x0f\x7f')
        with self.assertRaises(MQTTHeaderException):
            self.loop.run_until_complete(MQTTHeaderCodec.decode(stream))

    def test_decode_ko_with_length(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\x10\xff\xff\xff\xff\x7f')
        with self.assertRaises(MQTTHeaderException):
            self.loop.run_until_complete(MQTTHeaderCodec.decode(stream))
