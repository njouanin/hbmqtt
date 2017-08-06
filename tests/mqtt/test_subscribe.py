# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
import unittest

from hbmqtt.mqtt.subscribe import SubscribePacket, SubscribePayload
from hbmqtt.mqtt.packet import PacketIdVariableHeader
from hbmqtt.mqtt.constants import QOS_1, QOS_2
from hbmqtt.adapters import BufferReader


class SubscribePacketTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_from_stream(self):
        data = b'\x80\x0e\x00\x0a\x00\x03a/b\x01\x00\x03c/d\x02'
        stream = BufferReader(data)
        message = self.loop.run_until_complete(SubscribePacket.from_stream(stream))
        (topic, qos) = message.payload.topics[0]
        self.assertEqual(topic, 'a/b')
        self.assertEqual(qos, QOS_1)
        (topic, qos) = message.payload.topics[1]
        self.assertEqual(topic, 'c/d')
        self.assertEqual(qos, QOS_2)

    def test_to_stream(self):
        variable_header = PacketIdVariableHeader(10)
        payload = SubscribePayload(
            [
                ('a/b', QOS_1),
                ('c/d', QOS_2)
            ])
        publish = SubscribePacket(variable_header=variable_header, payload=payload)
        out = publish.to_bytes()
        self.assertEqual(out, b'\x82\x0e\x00\x0a\x00\x03a/b\x01\x00\x03c/d\x02')
