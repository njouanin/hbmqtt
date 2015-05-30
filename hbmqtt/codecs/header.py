# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio

from hbmqtt.codecs.utils import (
    bytes_to_hex_str,
    bytes_to_int,
    read_or_raise,
)
from hbmqtt.message import MQTTHeader, MessageType
from hbmqtt.codecs.errors import CodecException


class MQTTHeaderException(CodecException):
    pass

class MQTTHeaderCodec:
    @staticmethod
    @asyncio.coroutine
    def decode(reader) -> MQTTHeader:
        """
        Decode MQTT message fixed header from stream
        :param reader: Stream to read
        :return: FixedHeader instance
        """
        def get_message_type(byte):
            byte_type = (bytes_to_int(byte) & 0xf0) >> 4
            return MessageType(byte_type)

        def get_flags(data):
            byte = bytes_to_int(data)
            return byte & 0x0f

        @asyncio.coroutine
        def decode_remaining_length():
            """
            Decode message length according to MQTT specifications
            :return:
            """
            multiplier = 1
            value = 0
            length_bytes = b''
            while True:
                encoded_byte = yield from read_or_raise(reader, 1)
                length_bytes += encoded_byte
                int_byte = bytes_to_int(encoded_byte)
                value += (int_byte & 0x7f) * multiplier
                if (int_byte & 0x80) == 0:
                    break
                else:
                    multiplier *= 128
                    if multiplier > 128*128*128:
                        raise MQTTHeaderException("Invalid remaining length bytes:%s" % bytes_to_hex_str(length_bytes))
            return value

        b1 = yield from read_or_raise(reader, 1)
        msg_type = get_message_type(b1)
        if msg_type is MessageType.RESERVED_0 or msg_type is MessageType.RESERVED_15:
            raise MQTTHeaderException("Usage of control packet type %s is forbidden" % msg_type)
        flags = get_flags(b1)

        remain_length = yield from decode_remaining_length()
        return MQTTHeader(msg_type, flags, remain_length)

    @staticmethod
    @asyncio.coroutine
    def encode(header: MQTTHeader, writer):
        # To be done
        pass
