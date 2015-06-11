# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from hbmqtt.messages.packet import MQTTPacket, MQTTHeader, PacketType

class ReturnCode(Enum):
    CONNECTION_ACCEPTED = 0x00
    UNACCEPTABLE_PROTOCOL_VERSION = 0x01
    IDENTIFIER_REJECTED = 0x02
    SERVER_UNAVAILABLE = 0x03
    BAD_USERNAME_PASSWORD = 0x04
    NOT_AUTHORIZED = 0x05

class ConnackVariableHeader():
    def __init__(self, session_parent=None, return_code=None):
        self.session_parent = session_parent
        self.return_code = return_code

class ConnackPacket(MQTTPacket):
    def __init__(self, vh: ConnackVariableHeader):
        header = MQTTHeader(PacketType.CONNACK, 0x00)
        super().__init__(header)
        self.variable_header = vh
        self.payload = None
