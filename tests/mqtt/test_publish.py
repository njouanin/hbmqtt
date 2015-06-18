# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest

from hbmqtt.mqtt.publish import PublishPacket, PublishVariableHeader, PublishPayload
from hbmqtt.codecs import *

class PublishPacketTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_from_stream(self):
        data = b'\x3f\x09\x00\x05topic\x00\x0a0123456789'
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(data)
        stream.feed_eof()
        message = self.loop.run_until_complete(PublishPacket.from_stream(stream))
        self.assertEqual(message.variable_header.topic_name, 'topic')
        self.assertEqual(message.variable_header.packet_id, 10)
        self.assertEqual(message.fixed_header.qos, 0x03)
        self.assertTrue(message.fixed_header.dup_flag)
        self.assertTrue(message.fixed_header.retain_flag)
        self.assertTrue(message.payload.data, b'0123456789')

    def test_to_stream(self):
        variable_header = PublishVariableHeader('topic', 10)
        payload = PublishPayload(b'0123456789')
        publish = PublishPacket(variable_header=variable_header, payload=payload)
        out = publish.to_bytes()
        self.assertEqual(out, b'\x30\x13\x00\x05topic\x00\x0a0123456789')