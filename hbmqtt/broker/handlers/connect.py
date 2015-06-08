# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

import asyncio
from hbmqtt.message import MQTTMessage, ConnectMessage, ConnackMessage
from hbmqtt.broker.session import Session, ClientState
from hbmqtt.errors import BrokerException

class ConnectHandler:
    def __init__(self, broker):
        self.broker = broker

    @asyncio.coroutine
    def handle(self, message: ConnectMessage) -> MQTTMessage:
        session = None
        response = None

        # Check Protocol
        # protocol level (only MQTT 3.1.1 supported)
        if message.proto_level != 4:
            return ConnackMessage(False, ConnackMessage.ReturnCode.UNACCEPTABLE_PROTOCOL_VERSION)

        # No client ID provided
        if message.client_id is None or message.client_id == "":
            if message.is_clean_session():
                # [MQTT-3.1.3-6] and [MQTT-3.1.3-7]
                message.client_id = self.gen_client_id()
            else:
                # [MQTT-3.1.3-8] : Identifier rejected
                return ConnackMessage(False, ConnackMessage.ReturnCode.IDENTIFIER_REJECTED)

        if message.is_clean_session():
            try:
                self.broker.discard_session(message.client_id)
            except BrokerException:
                pass
            session = self.broker.create_session(message._remote_address, message._remote_port, message.client_id, message.is_clean_session())
            # [MQTT-3.2.2-1]
            response = ConnackMessage(False, 0)
        else:
            try:
                session = self.broker.get_session(message.client_id)
                if session.client_state == ClientState.CONNECTED:
                    # [MQTT-3.1.4-2]
                        # TODO : Add logging
                        return ConnackMessage(False, ConnackMessage.ReturnCode.IDENTIFIER_REJECTED)
                else:
                    # [MQTT-3.2.2-2]
                    response = ConnackMessage(True, 0)
            except BrokerException:
                session = self.broker.create_session(message._remote_address, message._remote_port, message.client_id, message.is_clean_session())
                response = ConnackMessage(False, 0)
            if session.client_state == ClientState.DISCONNECTED:
                self.broker.resume_session(session)
                session.client_state = ClientState.CONNECTED

        if message.is_will_flag():
            session.will_flag = True
            session.will_message = message.will_message
            session.will_qos = message.will_qos()
            session.will_retain = message.is_will_retain()

        session.keep_alive = message.keep_alive

        self.broker.save_session(session)
        return response

    def gen_client_id(self):
        import uuid
        return uuid.uuid4()