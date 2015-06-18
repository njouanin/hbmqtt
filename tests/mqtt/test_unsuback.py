# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest

from hbmqtt.mqtt.unsuback import UnsubackPacket
from hbmqtt.mqtt.packet import PacketIdVariableHeader
from hbmqtt.codecs import *

class SubscribePacketTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_from_stream(self):
        data = b'\xb0\x02\x00\x0a'
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(data)
        stream.feed_eof()
        message = self.loop.run_until_complete(UnsubackPacket.from_stream(stream))
        self.assertEqual(message.variable_header.packet_id, 10)

    def test_to_stream(self):
        variable_header = PacketIdVariableHeader(10)
        publish = UnsubackPacket(variable_header=variable_header)
        self.assertEqual(out, b'\xb0\x02\x00\x0a')