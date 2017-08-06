# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
import unittest

from hbmqtt.mqtt.publish import PublishPacket, PublishVariableHeader, PublishPayload
from hbmqtt.adapters import BufferReader
from hbmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2


class PublishPacketTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_from_stream_qos_0(self):
        data = b'\x31\x11\x00\x05topic0123456789'
        stream = BufferReader(data)
        message = self.loop.run_until_complete(PublishPacket.from_stream(stream))
        self.assertEqual(message.variable_header.topic_name, 'topic')
        self.assertEqual(message.variable_header.packet_id, None)
        self.assertFalse((message.fixed_header.flags >> 1) & 0x03)
        self.assertTrue(message.fixed_header.flags & 0x01)
        self.assertTrue(message.payload.data, b'0123456789')

    def test_from_stream_qos_2(self):
        data = b'\x37\x13\x00\x05topic\x00\x0a0123456789'
        stream = BufferReader(data)
        message = self.loop.run_until_complete(PublishPacket.from_stream(stream))
        self.assertEqual(message.variable_header.topic_name, 'topic')
        self.assertEqual(message.variable_header.packet_id, 10)
        self.assertTrue((message.fixed_header.flags >> 1) & 0x03)
        self.assertTrue(message.fixed_header.flags & 0x01)
        self.assertTrue(message.payload.data, b'0123456789')

    def test_to_stream_no_packet_id(self):
        variable_header = PublishVariableHeader('topic', None)
        payload = PublishPayload(b'0123456789')
        publish = PublishPacket(variable_header=variable_header, payload=payload)
        out = publish.to_bytes()
        self.assertEqual(out, b'\x30\x11\x00\x05topic0123456789')

    def test_to_stream_packet(self):
        variable_header = PublishVariableHeader('topic', 10)
        payload = PublishPayload(b'0123456789')
        publish = PublishPacket(variable_header=variable_header, payload=payload)
        out = publish.to_bytes()
        self.assertEqual(out, b'\x30\x13\x00\x05topic\00\x0a0123456789')

    def test_build(self):
        packet = PublishPacket.build('/topic', b'data', 1, False, QOS_0, False)
        self.assertEqual(packet.packet_id, 1)
        self.assertFalse(packet.dup_flag)
        self.assertEqual(packet.qos, QOS_0)
        self.assertFalse(packet.retain_flag)

        packet = PublishPacket.build('/topic', b'data', 1, False, QOS_1, False)
        self.assertEqual(packet.packet_id, 1)
        self.assertFalse(packet.dup_flag)
        self.assertEqual(packet.qos, QOS_1)
        self.assertFalse(packet.retain_flag)

        packet = PublishPacket.build('/topic', b'data', 1, False, QOS_2, False)
        self.assertEqual(packet.packet_id, 1)
        self.assertFalse(packet.dup_flag)
        self.assertEqual(packet.qos, QOS_2)
        self.assertFalse(packet.retain_flag)

        packet = PublishPacket.build('/topic', b'data', 1, True, QOS_0, False)
        self.assertEqual(packet.packet_id, 1)
        self.assertTrue(packet.dup_flag)
        self.assertEqual(packet.qos, QOS_0)
        self.assertFalse(packet.retain_flag)

        packet = PublishPacket.build('/topic', b'data', 1, True, QOS_1, False)
        self.assertEqual(packet.packet_id, 1)
        self.assertTrue(packet.dup_flag)
        self.assertEqual(packet.qos, QOS_1)
        self.assertFalse(packet.retain_flag)

        packet = PublishPacket.build('/topic', b'data', 1, True, QOS_2, False)
        self.assertEqual(packet.packet_id, 1)
        self.assertTrue(packet.dup_flag)
        self.assertEqual(packet.qos, QOS_2)
        self.assertFalse(packet.retain_flag)

        packet = PublishPacket.build('/topic', b'data', 1, False, QOS_0, True)
        self.assertEqual(packet.packet_id, 1)
        self.assertFalse(packet.dup_flag)
        self.assertEqual(packet.qos, QOS_0)
        self.assertTrue(packet.retain_flag)

        packet = PublishPacket.build('/topic', b'data', 1, False, QOS_1, True)
        self.assertEqual(packet.packet_id, 1)
        self.assertFalse(packet.dup_flag)
        self.assertEqual(packet.qos, QOS_1)
        self.assertTrue(packet.retain_flag)

        packet = PublishPacket.build('/topic', b'data', 1, False, QOS_2, True)
        self.assertEqual(packet.packet_id, 1)
        self.assertFalse(packet.dup_flag)
        self.assertEqual(packet.qos, QOS_2)
        self.assertTrue(packet.retain_flag)

        packet = PublishPacket.build('/topic', b'data', 1, True, QOS_0, True)
        self.assertEqual(packet.packet_id, 1)
        self.assertTrue(packet.dup_flag)
        self.assertEqual(packet.qos, QOS_0)
        self.assertTrue(packet.retain_flag)

        packet = PublishPacket.build('/topic', b'data', 1, True, QOS_1, True)
        self.assertEqual(packet.packet_id, 1)
        self.assertTrue(packet.dup_flag)
        self.assertEqual(packet.qos, QOS_1)
        self.assertTrue(packet.retain_flag)

        packet = PublishPacket.build('/topic', b'data', 1, True, QOS_2, True)
        self.assertEqual(packet.packet_id, 1)
        self.assertTrue(packet.dup_flag)
        self.assertEqual(packet.qos, QOS_2)
        self.assertTrue(packet.retain_flag)
