# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from hbmqtt.mqtt.packet import MQTTPacket, MQTTFixedHeader, PacketType
from hbmqtt.errors import HBMQTTException

class PingReqPacket(MQTTPacket):
    VARIABLE_HEADER = None
    PAYLOAD = None

    def __init__(self, fixed: MQTTFixedHeader=None, variable_header=None, payload=None):
        if fixed is None:
            header = MQTTFixedHeader(PacketType.PINGREQ, 0x00)
        else:
            if fixed.packet_type is not PacketType.DISCONNECT:
                raise HBMQTTException("Invalid fixed packet type %s for PingReqPacket init" % fixed.packet_type)
            header = fixed
        super().__init__(header)
        self.variable_header = None
        self.payload = None
