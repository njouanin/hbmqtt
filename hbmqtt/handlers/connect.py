# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

from hbmqtt.message import MQTTMessage, ConnectMessage

class ConnectHandle:
    @staticmethod
    def handle(message:ConnectMessage) -> MQTTMessage:
