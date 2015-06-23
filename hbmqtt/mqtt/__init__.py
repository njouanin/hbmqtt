# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from hbmqtt.mqtt.packet import MQTTFixedHeader, MQTTPacket, PacketType
from hbmqtt.mqtt.connect import ConnectPacket
from hbmqtt.mqtt.connack import ConnackPacket

def packet_class(fixed_header: MQTTFixedHeader):
    if fixed_header.packet_type == PacketType.CONNECT:
        return ConnectPacket
    if fixed_header.packet_type == PacketType.CONNACK:
        return ConnackPacket
