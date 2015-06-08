# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
import threading
import logging
from hbmqtt.errors import BrokerException
from transitions import Machine, MachineError
from hbmqtt.codecs.header import MQTTHeaderCodec
from hbmqtt.codecs.connect import ConnectMessage
from hbmqtt.message import MessageType, MQTTMessage, MQTTHeader
from hbmqtt.errors import MQTTException
from hbmqtt.broker.session import Session
from hbmqtt.broker.handlers import ConnectHandler


class Broker:
    states = ['new', 'starting', 'started', 'not_started', 'stopping', 'stopped', 'not_stopped', 'stopped']

    def __init__(self, host='127.0.0.1', port=1883, loop = None):
        self.logger = logging.getLogger(__name__)
        self._init_states()
        self.host = host
        self.port = port
        if loop is not None:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
        self._server = None
        self._loop_thread = None
        self._message_handlers = None
        self._codecs = None

    def _init_states(self):
        self.machine = Machine(states=Broker.states, initial='new')
        self.machine.add_transition(trigger='start', source='new', dest='starting')
        self.machine.add_transition(trigger='starting_fail', source='starting', dest='not_started')
        self.machine.add_transition(trigger='starting_success', source='starting', dest='started')
        self.machine.add_transition(trigger='shutdown', source='started', dest='stopping')
        self.machine.add_transition(trigger='stopping_success', source='stopping', dest='stopped')
        self.machine.add_transition(trigger='stopping_failure', source='stopping', dest='not_stopped')
        self.machine.add_transition(trigger='start', source='stopped', dest='starting')


    def _init_handlers(self):
        self._message_handlers = {
            MessageType.CONNECT, ConnectHandler(self),
        }

    def _init_codecs(self):
        """
        Init dict of MQTT message encoders/decoders
        :return:
        """
        self._codecs = {
            MessageType.CONNECT, ConnectMessage
        }

    def start(self):
        try:
            self.machine.start()
            self.logger.debug("Broker starting")
        except MachineError as me:
            self.logger.debug("Invalid method call at this moment: %s" % me)
            raise BrokerException("Broker instance can't be started: %s" % me)

        try:
            self._init_handlers()
            self._init_codecs()
            self._loop_thread = threading.Thread(target=self._run_server_loop, args=(self._loop,))
            self._loop_thread.setDaemon(True)
            self._loop_thread.start()
        except Exception as e:
            self.logger.error("Broker startup failed: %s" % e)
            self.machine.starting_fail()
            raise BrokerException("Broker instance can't be started: %s" % e)

    def shutdown(self):
        try:
            self.machine.shutdown()
        except MachineError as me:
            self.logger.debug("Invalid method call at this moment: %s" % me)
            raise BrokerException("Broker instance can't be stopped: %s" % me)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._loop_thread.join()
        self._server.close()
        self._loop.run_until_complete(self._server.wait_closed())
        self._loop.close()
        self._server = None
        self._message_handlers = None
        self._codecs = None
        self.machine.stopping_success()

    def _run_server_loop(self, loop):
        asyncio.set_event_loop(loop)
        coro = asyncio.start_server(self.client_connected, self.host, self.port)
        self._server = loop.run_until_complete(coro)
        self.logger.debug("Broker listening %s:%s" % (self.host, self.port))
        self.machine.starting_success()
        self.logger.debug("Broker started, ready to serve")
        loop.run_forever()

    @asyncio.coroutine
    def _handle_message(self, message: MQTTMessage) -> MQTTMessage:
        handler = self._message_handlers[message.mqtt_header.message_type]
        response = yield from handler.handle(message)
        return response

    @asyncio.coroutine
    def _decode_message(self, header: MQTTHeader, reader):
        decoder = self._codecs[header.message_type]
        message = yield from decoder.decode(header, reader)
        return message

    @asyncio.coroutine
    def _encode_message(self, message: MQTTMessage) -> bytes:
        encoder = self._codecs[message.mqtt_header.message_type]
        out = yield from encoder.encode(message)
        message.mqtt_header.remaining_length = len(out)
        encoded = MQTTHeaderCodec.encode(message.mqtt_header) + out
        return encoded

    @asyncio.coroutine
    def client_connected(self, reader, writer):
        # Handle connection
        try:
            header = yield from MQTTHeaderCodec.decode(reader)
            if header.message_type != MessageType.CONNECT:
                raise MQTTException("[MQTT-3.1.0-1] First Packet sent from the Client MUST be a CONNECT Packet")
            request = yield from self._decode_message(header.message_type, reader)

            (remote_address, remote_port) = writer.get_extra_info('peername')
            session = Session(remote_address, remote_port, request.client_id)

            response = self._handle_message(request)
            encoded_response = yield from self._encode_message(response)
            writer.write(encoded_response)
            yield from writer.drain()
        except MQTTException:
            return

        # Enter message loop
        while True:
            try:
                header = yield from MQTTHeaderCodec.decode(reader)
                request = yield from self._decode_message(header.message_type, reader)
                response = self._handle_message(request)
                encoded_response = yield from self._encode_message(response)
                writer.write(encoded_response)
                yield from writer.drain()
            except MQTTException:
                # End connection
                break
