# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from enum import Enum

from hbmqtt.errors import CodecException, MQTTException
from hbmqtt.codecs import *
import abc


class PacketType(Enum):
    RESERVED_0 = 0
    CONNECT = 1
    CONNACK = 2
    PUBLISH = 3
    PUBACK = 4
    PUBREC = 5
    PUBREL = 6
    PUBCOMP = 7
    SUBSCRIBE = 8
    SUBACK = 9
    UNSUBSCRIBE = 10
    UNSUBACK = 11
    PINGREQ = 12
    PINGRESP = 13
    DISCONNECT = 14
    RESERVED_15 = 15


def get_packet_type(byte):
    return PacketType(byte)


class MQTTFixedHeader:
    def __init__(self, packet_type, flags=0, length=0):
        if isinstance(packet_type, int):
            enum_type = packet_type
        else:
            enum_type = get_packet_type(packet_type)
        self.packet_type = enum_type
        self.remaining_length = length
        self.flags = flags

    def to_bytes(self):
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
            packet_type = (self.packet_type.value << 4) | self.flags
            out += int_to_bytes(packet_type)
        except OverflowError:
            raise CodecException('packet_type encoding exceed 1 byte length: value=%d', packet_type)

        encoded_length = encode_remaining_length(self.remaining_length)
        out += encoded_length

        return out

    @asyncio.coroutine
    def to_stream(self, writer: asyncio.StreamWriter):
        writer.write(self.to_bytes())
        yield from writer.drain()

    @classmethod
    @asyncio.coroutine
    def from_stream(cls, reader: asyncio.StreamReader):
        """
        Read and decode MQTT message fixed header from stream
        :return: FixedHeader instance
        """

        def decode_message_type(byte):
            byte_type = (bytes_to_int(byte) & 0xf0) >> 4
            return PacketType(byte_type)

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
                    if multiplier > 128 * 128 * 128:
                        raise MQTTException("Invalid remaining length bytes:%s" % bytes_to_hex_str(length_bytes))
            return value

        b1 = yield from read_or_raise(reader, 1)
        msg_type = decode_message_type(b1)
        if msg_type is PacketType.RESERVED_0 or msg_type is PacketType.RESERVED_15:
            raise MQTTException("Usage of control packet type %s is forbidden" % msg_type)
        flags = decode_flags(b1)

        remain_length = yield from decode_remaining_length()
        return cls(msg_type, flags, remain_length)

    def __repr__(self):
        return type(self).__name__ + '(type={0}, length={1}, flags={2})'.format(self.packet_type, self.remaining_length, hex(self.flags))

class MQTTVariableHeader(metaclass=abc.ABCMeta):
    def __init__(self):
        pass

    @asyncio.coroutine
    def to_stream(self, writer: asyncio.StreamWriter):
        writer.write(self.to_bytes())
        yield from writer.drain()

    @abc.abstractmethod
    def to_bytes(self):
        return

    @classmethod
    @asyncio.coroutine
    @abc.abstractclassmethod
    def from_stream(cls, reader: asyncio.StreamReader, fixed_header: MQTTFixedHeader):
        return


class MQTTPayload(metaclass=abc.ABCMeta):
    def __init__(self):
        pass

    @asyncio.coroutine
    def to_stream(self, writer: asyncio.StreamWriter):
        writer.write(self.to_bytes())
        yield from writer.drain()

    @abc.abstractmethod
    def to_bytes(self, fixed_header: MQTTFixedHeader, variable_header: MQTTVariableHeader):
        return

    @classmethod
    @asyncio.coroutine
    @abc.abstractclassmethod
    def from_stream(cls, reader: asyncio.StreamReader, fixed_header: MQTTFixedHeader,
                    variable_header: MQTTVariableHeader):
        return


class MQTTPacket:
    FIXED_HEADER = MQTTFixedHeader
    VARIABLE_HEADER = None
    PAYLOAD = None

    def __init__(self, fixed: MQTTFixedHeader, variable_header: MQTTVariableHeader=None, payload: MQTTPayload=None):
        self.fixed_header = fixed
        self.variable_header = variable_header
        self.payload = payload

    @asyncio.coroutine
    def to_stream(self, writer: asyncio.StreamWriter):
        writer.write(self.to_bytes())
        yield from writer.drain()

    def to_bytes(self):
        if self.variable_header:
            variable_header_bytes = self.variable_header.to_bytes()
        else:
            variable_header_bytes = b''
        if self.payload:
            payload_bytes = self.payload.to_bytes(self.fixed_header, self.variable_header)
        else:
            payload_bytes = b''

        self.fixed_header.remaining_length = len(variable_header_bytes) + len(payload_bytes)
        fixed_header_bytes = self.fixed_header.to_bytes()

        return fixed_header_bytes + variable_header_bytes + payload_bytes

    @classmethod
    @asyncio.coroutine
    def from_stream(cls, reader: asyncio.StreamReader):
        fixed_header = yield from cls.FIXED_HEADER.from_stream(reader)
        if cls.VARIABLE_HEADER:
            variable_header = yield from cls.VARIABLE_HEADER.from_stream(reader, fixed_header)
        else:
            variable_header = None
        if cls.PAYLOAD:
            payload = yield from cls.PAYLOAD.from_stream(reader, fixed_header, variable_header)
        else:
            payload = None

        return cls(fixed_header, variable_header, payload)

    def __repr__(self):
        return type(self).__name__ + '(fixed={0!r}, variable={1!r}, payload={2!r})'.format(self.fixed_header, self.variable_header, self.payload)


class PacketIdVariableHeader(MQTTVariableHeader):
    def __init__(self, packet_id):
        super().__init__()
        self.packet_id = packet_id

    def to_bytes(self):
        out = b''
        out += int_to_bytes(self.packet_id, 2)
        return out

    @classmethod
    def from_stream(cls, reader: asyncio.StreamReader, fixed_header: MQTTFixedHeader):
        packet_id = yield from decode_packet_id(reader)
        return cls(packet_id)
