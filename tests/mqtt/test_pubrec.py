# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
import unittest

from hbmqtt.mqtt.pubrec import PubrecPacket, PacketIdVariableHeader
from hbmqtt.adapters import BufferReader


class PubrecPacketTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_from_stream(self):
        data = b'\x50\x02\x00\x0a'
        stream = BufferReader(data)
        message = self.loop.run_until_complete(PubrecPacket.from_stream(stream))
        self.assertEqual(message.variable_header.packet_id, 10)

    def test_to_bytes(self):
        variable_header = PacketIdVariableHeader(10)
        publish = PubrecPacket(variable_header=variable_header)
        out = publish.to_bytes()
        self.assertEqual(out, b'\x50\x02\x00\x0a')
