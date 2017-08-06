# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from hbmqtt.errors import HBMQTTException
from hbmqtt.mqtt.packet import (
    CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE,
    SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT,
    MQTTFixedHeader)
from hbmqtt.mqtt.connect import ConnectPacket
from hbmqtt.mqtt.connack import ConnackPacket
from hbmqtt.mqtt.disconnect import DisconnectPacket
from hbmqtt.mqtt.pingreq import PingReqPacket
from hbmqtt.mqtt.pingresp import PingRespPacket
from hbmqtt.mqtt.publish import PublishPacket
from hbmqtt.mqtt.puback import PubackPacket
from hbmqtt.mqtt.pubrec import PubrecPacket
from hbmqtt.mqtt.pubrel import PubrelPacket
from hbmqtt.mqtt.pubcomp import PubcompPacket
from hbmqtt.mqtt.subscribe import SubscribePacket
from hbmqtt.mqtt.suback import SubackPacket
from hbmqtt.mqtt.unsubscribe import UnsubscribePacket
from hbmqtt.mqtt.unsuback import UnsubackPacket

packet_dict = {
    CONNECT: ConnectPacket,
    CONNACK: ConnackPacket,
    PUBLISH: PublishPacket,
    PUBACK: PubackPacket,
    PUBREC: PubrecPacket,
    PUBREL: PubrelPacket,
    PUBCOMP: PubcompPacket,
    SUBSCRIBE: SubscribePacket,
    SUBACK: SubackPacket,
    UNSUBSCRIBE: UnsubscribePacket,
    UNSUBACK: UnsubackPacket,
    PINGREQ: PingReqPacket,
    PINGRESP: PingRespPacket,
    DISCONNECT: DisconnectPacket
}


def packet_class(fixed_header: MQTTFixedHeader):
    try:
        cls = packet_dict[fixed_header.packet_type]
        return cls
    except KeyError:
        raise HBMQTTException("Unexpected packet Type '%s'" % fixed_header.packet_type)
