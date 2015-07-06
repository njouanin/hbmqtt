# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from hbmqtt.mqtt.packet import MQTTPacket, MQTTFixedHeader, PacketType, PacketIdVariableHeader
from hbmqtt.errors import HBMQTTException


class PubackPacket(MQTTPacket):
    VARIABLE_HEADER = PacketIdVariableHeader
    PAYLOAD = None

    def __init__(self, fixed: MQTTFixedHeader=None, variable_header: PacketIdVariableHeader=None, payload=None):
        if fixed is None:
            header = MQTTFixedHeader(PacketType.PUBACK, 0x00)
        else:
            if fixed.packet_type is not PacketType.PUBACK:
                raise HBMQTTException("Invalid fixed packet type %s for PubackPacket init" % fixed.packet_type)
            header = fixed
        super().__init__(header)
        self.variable_header = variable_header
        self.payload = None

    @classmethod
    def build(cls, packet_id: int):
        v_header = PacketIdVariableHeader(packet_id)
        packet = PubackPacket(variable_header=v_header, payload=None)
        return packet
