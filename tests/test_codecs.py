# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio

from hbmqtt.codecs import (
    bytes_to_hex_str,
    bytes_to_int,
    decode_string,
    encode_string,
)


class TestCodecs(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_bytes_to_hex_str(self):
        ret = bytes_to_hex_str(b'\x7f')
        self.assertEqual(ret, '0x7f')

    def test_bytes_to_int(self):
        ret = bytes_to_int(b'\x7f')
        self.assertEqual(ret, 127)
        ret = bytes_to_int(b'\xff\xff')
        self.assertEqual(ret, 65535)

    def test_decode_string(self):
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(b'\x00\x02AA')
        ret = self.loop.run_until_complete(decode_string(stream))
        self.assertEqual(ret, 'AA')

    def test_encode_string(self):
        encoded = encode_string('AA')
        self.assertEqual(b'\x00\x02AA', encoded)
