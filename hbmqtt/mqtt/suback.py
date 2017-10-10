# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio

from hbmqtt.mqtt.packet import MQTTPacket, MQTTFixedHeader, SUBACK, PacketIdVariableHeader, MQTTPayload, MQTTVariableHeader
from hbmqtt.errors import HBMQTTException, NoDataException
from hbmqtt.adapters import ReaderAdapter
from hbmqtt.codecs import bytes_to_int, int_to_bytes, read_or_raise


class SubackPayload(MQTTPayload):

    __slots__ = ('return_codes',)

    RETURN_CODE_00 = 0x00
    RETURN_CODE_01 = 0x01
    RETURN_CODE_02 = 0x02
    RETURN_CODE_80 = 0x80

    def __init__(self, return_codes=[]):
        super().__init__()
        self.return_codes = return_codes

    def __repr__(self):
        return type(self).__name__ + '(return_codes={0})'.format(repr(self.return_codes))

    def to_bytes(self, fixed_header: MQTTFixedHeader, variable_header: MQTTVariableHeader):
        out = b''
        for return_code in self.return_codes:
            out += int_to_bytes(return_code, 1)
        return out

    @classmethod
    @asyncio.coroutine
    def from_stream(cls, reader: ReaderAdapter, fixed_header: MQTTFixedHeader,
                    variable_header: MQTTVariableHeader):
        return_codes = []
        bytes_to_read = fixed_header.remaining_length - variable_header.bytes_length
        for i in range(0, bytes_to_read):
            try:
                return_code_byte = yield from read_or_raise(reader, 1)
                return_code = bytes_to_int(return_code_byte)
                return_codes.append(return_code)
            except NoDataException:
                break
        return cls(return_codes)


class SubackPacket(MQTTPacket):
    VARIABLE_HEADER = PacketIdVariableHeader
    PAYLOAD = SubackPayload

    def __init__(self, fixed: MQTTFixedHeader=None, variable_header: PacketIdVariableHeader=None, payload=None):
        if fixed is None:
            header = MQTTFixedHeader(SUBACK, 0x00)
        else:
            if fixed.packet_type is not SUBACK:
                raise HBMQTTException("Invalid fixed packet type %s for SubackPacket init" % fixed.packet_type)
            header = fixed

        super().__init__(header)
        self.variable_header = variable_header
        self.payload = payload

    @classmethod
    def build(cls, packet_id, return_codes):
        variable_header = cls.VARIABLE_HEADER(packet_id)
        payload = cls.PAYLOAD(return_codes)
        return cls(variable_header=variable_header, payload=payload)
