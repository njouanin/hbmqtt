# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
import unittest

from hbmqtt.mqtt.unsubscribe import UnsubscribePacket, UnubscribePayload
from hbmqtt.mqtt.packet import PacketIdVariableHeader
from hbmqtt.adapters import BufferReader


class UnsubscribePacketTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_from_stream(self):
        data = b'\xa2\x0c\x00\n\x00\x03a/b\x00\x03c/d'
        stream = BufferReader(data)
        message = self.loop.run_until_complete(UnsubscribePacket.from_stream(stream))
        self.assertEqual(message.payload.topics[0], 'a/b')
        self.assertEqual(message.payload.topics[1], 'c/d')

    def test_to_stream(self):
        variable_header = PacketIdVariableHeader(10)
        payload = UnubscribePayload(['a/b', 'c/d'])
        publish = UnsubscribePacket(variable_header=variable_header, payload=payload)
        out = publish.to_bytes()
        self.assertEqual(out, b'\xa2\x0c\x00\n\x00\x03a/b\x00\x03c/d')
