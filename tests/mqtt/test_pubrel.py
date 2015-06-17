# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest

from hbmqtt.mqtt.pubrel import PubrelPacket, PubrelVariableHeader
from hbmqtt.codecs import *

class PubrelPacketTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_from_stream(self):
        data = b'\x60\x02\x00\x0a'
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(data)
        message = self.loop.run_until_complete(PubrelPacket.from_stream(stream))
        self.assertEqual(message.variable_header.packet_id, 10)

    def test_to_bytes(self):
        variable_header = PubrelVariableHeader(10)
        publish = PubrelPacket(variable_header=variable_header)
        out = publish.to_bytes()
        self.assertEqual(out, b'\x60\x02\x00\x0a')