# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from hbmqtt.mqtt.packet import MQTTPacket, MQTTFixedHeader, PacketType, MQTTVariableHeader
from hbmqtt.errors import HBMQTTException
from hbmqtt.codecs import *


class PubrecVariableHeader(MQTTVariableHeader):
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


class PubrecPacket(MQTTPacket):
    VARIABLE_HEADER = PubrecVariableHeader
    PAYLOAD = None

    def __init__(self, fixed: MQTTFixedHeader=None, variable_header: PubrecVariableHeader=None, payload=None):
        if fixed is None:
            header = MQTTFixedHeader(PacketType.PUBREC, 0x00)
        else:
            if fixed.packet_type is not PacketType.PUBREC:
                raise HBMQTTException("Invalid fixed packet type %s for PubrecPacket init" % fixed.packet_type)
            header = fixed
        super().__init__(header)
        self.variable_header = variable_header
        self.payload = None
