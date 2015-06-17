# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest

from hbmqtt.mqtt.subscribe import SubscribePacket, SubscribePayload
from hbmqtt.mqtt.packet import PacketIdVariableHeader
from hbmqtt.codecs import *

class SubscribePacketTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_from_stream(self):
        data = b'\x80\x0e\x00\x0a\x00\x03a/b\x01\x00\x03c/d\x02'
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(data)
        stream.feed_eof()
        message = self.loop.run_until_complete(SubscribePacket.from_stream(stream))
        self.assertEqual(message.payload.topics[0]['filter'], 'a/b')
        self.assertEqual(message.payload.topics[0]['qos'], 0x01)
        self.assertEqual(message.payload.topics[1]['filter'], 'c/d')
        self.assertEqual(message.payload.topics[1]['qos'], 0x02)

    def test_to_stream(self):
        variable_header = PacketIdVariableHeader(10)
        payload = SubscribePayload(
            [
                {'filter': 'a/b', 'qos': 0x01},
                {'filter': 'c/d', 'qos': 0x02}
            ])
        publish = SubscribePacket(variable_header=variable_header, payload=payload)
        out = publish.to_bytes()
        self.assertEqual(out, b'\x80\x0e\x00\x0a\x00\x03a/b\x01\x00\x03c/d\x02')