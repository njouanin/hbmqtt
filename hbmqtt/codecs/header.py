# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio

from hbmqtt.codecs.utils import (
    bytes_to_hex_str,
    bytes_to_int,
    int_to_bytes,
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
        def decode_message_type(byte):
            byte_type = (bytes_to_int(byte) & 0xf0) >> 4
            return MessageType(byte_type)

        def decode_flags(data):
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
        msg_type = decode_message_type(b1)
        if msg_type is MessageType.RESERVED_0 or msg_type is MessageType.RESERVED_15:
            raise MQTTHeaderException("Usage of control packet type %s is forbidden" % msg_type)
        flags = decode_flags(b1)

        remain_length = yield from decode_remaining_length()
        return MQTTHeader(msg_type, flags, remain_length)

    @staticmethod
    def encode(header: MQTTHeader) -> bytes:
        def encode_remaining_length(length: int):
            encoded = b''
            while True:
                length_byte = length % 0x80
                length //= 0x80
                if length > 0:
                    length_byte |= 0x80
                encoded += int_to_bytes(length_byte)
                if length <= 0:
                    break
            return encoded

        out = b''
        packet_type = 0
        try:
            packet_type = (header.message_type.value << 4) | header.flags
            out += int_to_bytes(packet_type)
        except OverflowError:
            raise CodecException('packet_size encoding exceed 1 byte length: value=%d', packet_type)

        try:
            encoded_length = encode_remaining_length(header.remaining_length)
            out += encoded_length
        except OverflowError:
            raise CodecException('message length encoding exceed 1 byte length: value=%d', header.remaining_length)

        return out
