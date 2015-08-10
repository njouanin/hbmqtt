# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from hbmqtt.errors import CodecException, MQTTException
from hbmqtt.codecs import *
from hbmqtt.adapters import ReaderAdapter, WriterAdapter
import abc


RESERVED_0 = 0x00
CONNECT = 0x01
CONNACK = 0x02
PUBLISH = 0x03
PUBACK = 0x04
PUBREC = 0x05
PUBREL = 0x06
PUBCOMP = 0x07
SUBSCRIBE = 0x08
SUBACK = 0x09
UNSUBSCRIBE = 0x0a
UNSUBACK = 0x0b
PINGREQ = 0x0c
PINGRESP = 0x0d
DISCONNECT = 0x0e
RESERVED_15 = 0x0f


class MQTTFixedHeader:
    def __init__(self, packet_type, flags=0, length=0):
        self.packet_type = packet_type
        self.remaining_length = length
        self.flags = flags

    def to_bytes(self):
        def encode_remaining_length(length: int):
            encoded = bytearray()
            while True:
                length_byte = length % 0x80
                length //= 0x80
                if length > 0:
                    length_byte |= 0x80
                encoded.append(length_byte)
                if length <= 0:
                    break
            return encoded

        out = bytearray()
        packet_type = 0
        try:
            packet_type = (self.packet_type << 4) | self.flags
            out.append(packet_type)
        except OverflowError:
            raise CodecException('packet_type encoding exceed 1 byte length: value=%d', packet_type)

        encoded_length = encode_remaining_length(self.remaining_length)
        out.extend(encoded_length)

        return out

    @asyncio.coroutine
    def to_stream(self, writer: WriterAdapter):
        writer.write(self.to_bytes())

    @property
    def bytes_length(self):
        return len(self.to_bytes())

    @classmethod
    def from_stream(cls, reader: ReaderAdapter):
        """
        Read and decode MQTT message fixed header from stream
        :return: FixedHeader instance
        """
        @asyncio.coroutine
        def decode_remaining_length():
            """
            Decode message length according to MQTT specifications
            :return:
            """
            multiplier = 1
            value = 0
            buffer = b''
            while True:
                encoded_byte = yield from reader.read(1)
                buffer += encoded_byte
                int_byte = bytes_to_int(encoded_byte)
                value += (int_byte & 0x7f) * multiplier
                if (int_byte & 0x80) == 0:
                    break
                else:
                    multiplier *= 128
                    if multiplier > 128 * 128 * 128:
                        raise MQTTException("Invalid remaining length bytes:%s" % bytes_to_hex_str(buffer))
            return value

        try:
            byte1 = yield from read_or_raise(reader, 1)
            int1 = bytes_to_int(byte1)
            msg_type = (int1 & 0xf0) >> 4
            flags = int1 & 0x0f
            remain_length = yield from decode_remaining_length()

            return cls(msg_type, flags, remain_length)
        except NoDataException:
            return None

    def __repr__(self):
        return type(self).__name__ + '(type={0}, length={1}, flags={2})'.\
            format(self.packet_type, self.remaining_length, hex(self.flags))


class MQTTVariableHeader(metaclass=abc.ABCMeta):
    def __init__(self):
        pass

    @asyncio.coroutine
    def to_stream(self, writer: asyncio.StreamWriter):
        writer.write(self.to_bytes())
        yield from writer.drain()

    def to_bytes(self) -> bytes:
        """
        Serialize header data to a byte array conforming to MQTT protocol
        :return: serialized data
        """

    @property
    def bytes_length(self):
        return len(self.to_bytes())

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

    def to_bytes(self) -> bytes:
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
    def from_stream(cls, reader: ReaderAdapter, fixed_header=None, variable_header=None):
        if fixed_header is None:
            fixed_header = yield from cls.FIXED_HEADER.from_stream(reader)
        if cls.VARIABLE_HEADER:
            if variable_header is None:
                variable_header = yield from cls.VARIABLE_HEADER.from_stream(reader, fixed_header)
        else:
            variable_header = None
        if cls.PAYLOAD:
            payload = yield from cls.PAYLOAD.from_stream(reader, fixed_header, variable_header)
        else:
            payload = None

        if fixed_header and not variable_header and not payload:
            return cls(fixed_header)
        elif fixed_header and not payload:
            return cls(fixed_header, variable_header)
        else:
            return cls(fixed_header, variable_header, payload)

    @property
    def bytes_length(self):
        return len(self.to_bytes())

    def __repr__(self):
        return type(self).__name__ + '(fixed={0!r}, variable={1!r}, payload={2!r})'.\
            format(self.fixed_header, self.variable_header, self.payload)


class PacketIdVariableHeader(MQTTVariableHeader):
    def __init__(self, packet_id):
        super().__init__()
        self.packet_id = packet_id

    def to_bytes(self):
        out = b''
        out += int_to_bytes(self.packet_id, 2)
        return out

    @classmethod
    def from_stream(cls, reader: ReaderAdapter, fixed_header: MQTTFixedHeader):
        packet_id = yield from decode_packet_id(reader)
        return cls(packet_id)

    def __repr__(self):
        return type(self).__name__ + '(packet_id={0})'.format(self.packet_id)
