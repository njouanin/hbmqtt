# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio
from hbmqtt.codecs.connect import ConnectCodec, ConnectException
from hbmqtt.message import MessageType, MQTTHeader, ConnectMessage

class TestConnectCodec(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_decode_ok(self):
        data = b'\x00\x04MQTT\x04\xce\x00\x00\x00\x0a0123456789\x00\x09WillTopic\x00\x0bWillMessage\x00\x04user\x00\x08password'
        header = MQTTHeader(MessageType.CONNECT, 0x00, len(data))
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(data)
        message = self.loop.run_until_complete(ConnectCodec.decode(header, stream))
        self.assertEqual(message.proto_name, "MQTT")
        self.assertEqual(message.proto_level, 4)
        self.assertTrue(message.is_user_name_flag())
        self.assertTrue(message.is_password_flag())
        self.assertFalse(message.is_will_retain())
        self.assertEqual(message.will_qos(), 1)
        self.assertTrue(message.is_will_flag())
        self.assertTrue(message.is_clean_session())
        self.assertFalse(message.is_reserved_flag())

    def test_decode_fail_protocol_name(self):
        data = b'\x00\x04TTQM\x04\xce\x00\x00\x00\x0a0123456789\x00\x09WillTopic\x00\x0bWillMessage\x00\x04user\x00\x08password'
        header = MQTTHeader(MessageType.CONNECT, 0x00, len(data))
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(data)
        with self.assertRaises(ConnectException):
            self.loop.run_until_complete(ConnectCodec.decode(header, stream))

    def test_decode_fail_protocol_level(self):
        data = b'\x00\x04MQTT\x05\xce\x00\x00\x00\x0a0123456789\x00\x09WillTopic\x00\x0bWillMessage\x00\x04user\x00\x08password'
        header = MQTTHeader(MessageType.CONNECT, 0x00, len(data))
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(data)
        with self.assertRaises(ConnectException):
            self.loop.run_until_complete(ConnectCodec.decode(header, stream))

    def test_decode_fail_reserved_flag(self):
        data = b'\x00\x04MQTT\x04\xcf\x00\x00\x00\x0a0123456789\x00\x09WillTopic\x00\x0bWillMessage\x00\x04user\x00\x08password'
        header = MQTTHeader(MessageType.CONNECT, 0x00, len(data))
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(data)
        with self.assertRaises(ConnectException):
            self.loop.run_until_complete(ConnectCodec.decode(header, stream))

    def test_decode_fail_miss_clientId(self):
        data = b'\x00\x04MQTT\x04\xce\x00\x00'
        header = MQTTHeader(MessageType.CONNECT, 0x00, len(data))
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(data)
        stream.feed_eof()
        with self.assertRaises(ConnectException):
            self.loop.run_until_complete(ConnectCodec.decode(header, stream))

    def test_decode_fail_miss_willtopic(self):
        data = b'\x00\x04MQTT\x04\xce\x00\x00\x00\x0a0123456789'
        header = MQTTHeader(MessageType.CONNECT, 0x00, len(data))
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(data)
        stream.feed_eof()
        with self.assertRaises(ConnectException):
            self.loop.run_until_complete(ConnectCodec.decode(header, stream))

    def test_decode_fail_miss_username(self):
        data = b'\x00\x04MQTT\x04\xce\x00\x00\x00\x0a0123456789\x00\x09WillTopic\x00\x0bWillMessage'
        header = MQTTHeader(MessageType.CONNECT, 0x00, len(data))
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(data)
        stream.feed_eof()
        with self.assertRaises(ConnectException):
            self.loop.run_until_complete(ConnectCodec.decode(header, stream))

    def test_decode_fail_miss_password(self):
        data = b'\x00\x04MQTT\x04\xce\x00\x00\x00\x0a0123456789\x00\x09WillTopic\x00\x0bWillMessage\x00\x04user'
        header = MQTTHeader(MessageType.CONNECT, 0x00, len(data))
        stream = asyncio.StreamReader(loop=self.loop)
        stream.feed_data(data)
        stream.feed_eof()
        with self.assertRaises(ConnectException):
            self.loop.run_until_complete(ConnectCodec.decode(header, stream))

    def test_encode(self):
        header = MQTTHeader(MessageType.CONNECT, 0x00, 0)
        message = ConnectMessage(header, 0xce, 0, 'MQTT', 4)
        message.client_id = '0123456789'
        message.will_topic = 'WillTopic'
        message.will_message = 'WillMessage'
        message.user_name = 'user'
        message.password = 'password'
        encoded = ConnectCodec.encode(message)
        self.assertEqual(encoded, b'\x00\x04MQTT\x04\xce\x00\x00\x00\x0a0123456789\x00\x09WillTopic\x00\x0bWillMessage\x00\x04user\x00\x08password')
