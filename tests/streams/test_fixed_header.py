# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio
from hbmqtt.streams.fixed_header import FixedHeaderStream
from hbmqtt.streams.errors import FixedHeaderException

class TestFixedHeader(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_get_message_type(self):
        m_type = FixedHeaderStream.get_message_type(b'\x10')
        self.assertEqual(m_type, 1)

    def test_get_flags(self):
        (dup_flag, qos, retain_flag) = FixedHeaderStream.get_flags(b'\x1f')
        self.assertTrue(dup_flag)
        self.assertEqual(qos, 3)
        self.assertTrue(retain_flag)

    def test_decode_remaining_length1(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\x7f')
        s = FixedHeaderStream()
        length = self.loop.run_until_complete(s.decode_remaining_length(stream))
        self.assertEqual(length, 127)

    def test_decode_remaining_length2(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\xff\x7f')
        s = FixedHeaderStream()
        length = self.loop.run_until_complete(s.decode_remaining_length(stream))
        self.assertEqual(length, 16383)

    def test_decode_remaining_length3(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\xff\xff\x7f')
        s = FixedHeaderStream()
        length = self.loop.run_until_complete(s.decode_remaining_length(stream))
        self.assertEqual(length, 2097151)

    def test_decode_remaining_length4(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\xff\xff\xff\x7f')
        s = FixedHeaderStream()
        length = self.loop.run_until_complete(s.decode_remaining_length(stream))
        self.assertEqual(length, 268435455)

    def test_decode_remaining_length5(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\xff\xff\xff\xff\x7f')
        s = FixedHeaderStream()
        with self.assertRaises(FixedHeaderException):
            self.loop.run_until_complete(s.decode_remaining_length(stream))
