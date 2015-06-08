# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

from hbmqtt.message import MQTTMessage, ConnectMessage

class ConnectHandler:
    def __init__(self, broker):
        self._broker = broker

    def handle(message: ConnectMessage) -> MQTTMessage:
        # TBD
        pass