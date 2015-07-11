# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging
import asyncio

from transitions import Machine, MachineError
from hbmqtt.session import Session
from hbmqtt.mqtt.protocol.broker_handler import BrokerProtocolHandler
from hbmqtt.mqtt.connect import ConnectPacket
from hbmqtt.mqtt.connack import ConnackPacket, ReturnCode
from hbmqtt.errors import HBMQTTException
from hbmqtt.utils import format_client_message, gen_client_id


_defaults = {
    'bind-address': 'localhost',
    'bind-port': 1883
}


class BrokerException(BaseException):
    pass


class Broker:
    states = ['new', 'starting', 'started', 'not_started', 'stopping', 'stopped', 'not_stopped', 'stopped']

    def __init__(self, config=None, loop=None):
        self.logger = logging.getLogger(__name__)
        self.config = _defaults
        if config is not None:
            self.config.update(config)

        if loop is not None:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()

        self._server = None
        self._handlers = []
        self._init_states()
        self._sessions = dict()

    def _init_states(self):
        self.machine = Machine(states=Broker.states, initial='new')
        self.machine.add_transition(trigger='start', source='new', dest='starting')
        self.machine.add_transition(trigger='starting_fail', source='starting', dest='not_started')
        self.machine.add_transition(trigger='starting_success', source='starting', dest='started')
        self.machine.add_transition(trigger='shutdown', source='started', dest='stopping')
        self.machine.add_transition(trigger='stopping_success', source='stopping', dest='stopped')
        self.machine.add_transition(trigger='stopping_failure', source='stopping', dest='not_stopped')
        self.machine.add_transition(trigger='start', source='stopped', dest='starting')

    @asyncio.coroutine
    def start(self):
        try:
            self.machine.start()
            self.logger.debug("Broker starting")
        except MachineError as me:
            self.logger.debug("Invalid method call at this moment: %s" % me)
            raise BrokerException("Broker instance can't be started: %s" % me)

        try:
            self._server = yield from asyncio.start_server(self.client_connected,
                                                           self.config['bind-address'],
                                                           self.config['bind-port'],
                                                           loop=self._loop)
            self.logger.info("Broker listening on %s:%d" % (self.config['bind-address'], self.config['bind-port']))
            self.machine.starting_success()
        except Exception as e:
            self.logger.error("Broker startup failed: %s" % e)
            self.machine.starting_fail()
            raise BrokerException("Broker instance can't be started: %s" % e)

    @asyncio.coroutine
    def shutdown(self):
        try:
            self.machine.shutdown()
        except MachineError as me:
            self.logger.debug("Invalid method call at this moment: %s" % me)
            raise BrokerException("Broker instance can't be stopped: %s" % me)
        self._server.close()
        self.logger.debug("Broker closing")
        yield from self._server.wait_closed()
        self.logger.info("Broker closed")
        self.machine.stopping_success()

    @asyncio.coroutine
    def client_connected(self, reader, writer):
        self.logger.info(repr(writer.get_extra_info('peername')))
        extra_info = writer.get_extra_info('peername')
        remote_address = extra_info[0]
        remote_port = extra_info[1]
        self.logger.debug("Connection from %s:%d" % (remote_address, remote_port))

        # Wait for first packet and expect a CONNECT
        connect = None
        try:
            connect = yield from ConnectPacket.from_stream(reader)
            self.check_connect(connect)
        except HBMQTTException as exc:
            self.logger.warn("[MQTT-3.1.0-1] %s: Can't read first packet an CONNECT: %s" %
                             (format_client_message(address=remote_address, port=remote_port), exc))
            writer.close()
            return
        except BrokerException as be:
            self.logger.error('Invalid connection from %s : %s' %
                              (format_client_message(address=remote_address, port=remote_port), be))
            writer.close()
            return

        connack = None
        if connect.variable_header.proto_level != 4:
            # only MQTT 3.1.1 supported
            self.logger.error('Invalid protocol from %s: %d' %
                              (format_client_message(address=remote_address, port=remote_port),
                               connect.variable_header.protocol_level))
            connack = ConnackPacket.build(0, ReturnCode.UNACCEPTABLE_PROTOCOL_VERSION)  # [MQTT-3.2.2-4] session_parent=0
        elif connect.variable_header.username_flag and connect.payload.username is None:
            self.logger.error('Invalid username from %s' %
                              (format_client_message(address=remote_address, port=remote_port)))
            connack = ConnackPacket.build(0, ReturnCode.BAD_USERNAME_PASSWORD)  # [MQTT-3.2.2-4] session_parent=0
        elif connect.variable_header.password_flag and connect.payload.password is None:
            self.logger.error('Invalid password %s' % (format_client_message(address=remote_address, port=remote_port)))
            connack = ConnackPacket.build(0, ReturnCode.BAD_USERNAME_PASSWORD)  # [MQTT-3.2.2-4] session_parent=0
        elif connect.variable_header.clean_session_flag == False and connect.payload.client_id is None:
            self.logger.error('[MQTT-3.1.3-8] [MQTT-3.1.3-9] %s: No client Id provided (cleansession=0)' %
                              format_client_message(address=remote_address, port=remote_port))
            connack = ConnackPacket.build(0, ReturnCode.IDENTIFIER_REJECTED)
            self.logger.debug(" -out-> " + repr(connack))
        if connack is not None:
            self.logger.debug(" -out-> " + repr(connack))
            yield from connack.to_stream(writer)
            writer.close()
            return

        new_session = None
        if connect.variable_header.clean_session_flag:
            client_id = connect.payload.client_id
            if client_id is not None and client_id in self._sessions:
                del self._sessions[client_id]
            new_session = Session()
            new_session.parent = 0
        else:
            # Get session from cache
            client_id = connect.payload.client_id
            if client_id in self._sessions:
                new_session = self._sessions[client_id]
                new_session.parent = 1
            else:
                new_session = Session()
                new_session.parent = 0

        if new_session.client_id is None:
            # Generate client ID
            new_session.client_id = gen_client_id()
        new_session.remote_address = remote_address
        new_session.remote_port = remote_port
        new_session.clean_session = connect.variable_header.clean_session_flag
        new_session.will_flag = connect.variable_header.will_flag
        new_session.will_retain = connect.variable_header.will_retain_flag
        new_session.will_qos = connect.variable_header.will_qos
        new_session.will_topic = connect.payload.will_topic
        new_session.will_message = connect.payload.will_message
        new_session.username = connect.payload.username
        new_session.password = connect.payload.password
        new_session.client_id = connect.payload.client_id
        new_session.keep_alive = connect.variable_header.keep_alive

        new_session.reader = reader
        new_session.writer = writer

        if self.authenticate(new_session):
            connack = ConnackPacket.build(new_session.parent, ReturnCode.CONNECTION_ACCEPTED)
            self.logger.info('%s : connection accepted' % format_client_message(session=new_session))
            self.logger.debug(" -out-> " + repr(connack))
            yield from connack.to_stream(writer)
        else:
            connack = ConnackPacket.build(new_session.parent, ReturnCode.NOT_AUTHORIZED)
            self.logger.info('%s : connection refused' % format_client_message(session=new_session))
            self.logger.debug(" -out-> " + repr(connack))
            yield from connack.to_stream(writer)
            writer.close()
            return

        new_session.machine.connect()
        handler = BrokerProtocolHandler(new_session, self._loop)
        self._handlers.append(handler)
        self.logger.debug("Start messages handling")
        yield from handler.start()
        self.logger.debug("Wait for disconnect")
        yield from handler.wait_disconnect()
        self.logger.debug("Client disconnected")
        yield from handler.stop()
        new_session.machine.disconnect()

    @asyncio.coroutine
    def check_connect(self, connect: ConnectPacket):
        if connect.payload.client_id is None:
            raise BrokerException('[[MQTT-3.1.3-3]] : Client identifier must be present' )

        if connect.variable_header.will_flag:
            if connect.payload.will_topic is None or connect.payload.will_message is None:
                raise BrokerException('will flag set, but will topic/message not present in payload')

        if connect.variable_header.reserved_flag:
            raise BrokerException('[MQTT-3.1.2-3] CONNECT reserved flag must be set to 0')

    def authenticate(self, session: Session):
        # TODO : Handle client authentication here
        return True
