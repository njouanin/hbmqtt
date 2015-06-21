__author__ = 'nico'

import logging
import asyncio
from urllib.parse import urlparse

from transitions import Machine, MachineError

from hbmqtt.utils import not_in_dict_or_none
from hbmqtt.session import Session, SessionState
from hbmqtt.mqtt.connect import ConnectPacket
from hbmqtt.mqtt.connack import ConnackPacket, ReturnCode
from hbmqtt.mqtt.disconnect import DisconnectPacket
from hbmqtt.mqtt.publish import PublishPacket
from hbmqtt.mqtt.puback import PubackPacket
from hbmqtt.mqtt.pubrec import PubrecPacket
from hbmqtt.mqtt.pubrel import PubrelPacket
from hbmqtt.mqtt.pubcomp import PubcompPacket
from hbmqtt.mqtt.pingreq import PingReqPacket
from hbmqtt.mqtt.pingresp import PingRespPacket
from hbmqtt.mqtt.subscribe import SubscribePacket
from hbmqtt.mqtt.suback import SubackPacket
from hbmqtt.errors import MQTTException

_defaults = {
    'keep_alive': 60,
    'ping_delay': 1,
    'default_qos': 0,
    'default_retain': False
}


class ClientException(BaseException):
    pass


def gen_client_id():
    import uuid
    return str(uuid.uuid4())


class MQTTClient:
    states = ['new', 'connecting', 'connected', 'idle', 'disconnected']

    def __init__(self, client_id=None, config={}, loop=None):
        """

        :param config: Example yaml config
            broker:
                host: localhost
                port: 1883
                scheme: mqtt
                username: xxx
                password: yyy
                # OR
                uri: mqtt:xxx@yyy//localhost:1883/
                # OR a mix ot both
            keep_alive: 60
            clean_session: true
            will:
                retain: false
                topic: some/topic
                message: Will message
                qos: 0
            default_qos: 0
            default_retain: false
            topics:
                a/b:
                    qos: 2
                    retain: true
        :param loop:
        :return:
        """
        self.logger = logging.getLogger(__name__)
        self.config = config.copy()
        self.config.update(_defaults)
        if client_id is not None:
            self.client_id = client_id
        else:
            self.client_id = gen_client_id()
            self.logger.debug("Using generated client ID : %s" % self.client_id)

        self._init_states()
        if loop is not None:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
        self._ping_handle = None
        self._session = None

    def _init_states(self):
        self.machine = Machine(states=MQTTClient.states, initial='new')
        self.machine.add_transition(trigger='connect', source='new', dest='connecting')
        self.machine.add_transition(trigger='connect', source='disconnected', dest='connecting')
        self.machine.add_transition(trigger='connect_fail', source='connecting', dest='disconnected')
        self.machine.add_transition(trigger='connect_success', source='connecting', dest='connected')
        self.machine.add_transition(trigger='idle', source='connected', dest='idle')
        self.machine.add_transition(trigger='disconnect', source='idle', dest='disconnected')
        self.machine.add_transition(trigger='disconnect', source='connected', dest='disconnected')

    @asyncio.coroutine
    def connect(self, host=None, port=None, username=None, password=None, uri=None, clean_session=None):
        try:
            self.machine.connect()
            self._session = self._init_session(host, port, username, password, uri, clean_session)
            self.logger.debug("Connect with session parameters: %s" % self._session)

            yield from self._connect_coro()
            self.machine.connect_success()
            self._keep_alive()
        except MachineError:
            msg = "Connect call incompatible with client current state '%s'" % self.machine.current_state
            self.logger.warn(msg)
            self.machine.connect_fail()
            raise ClientException(msg)
        except Exception as e:
            self.machine.connect_fail()
            self.logger.warn("Connection failed: %s " % e)
            raise ClientException("Connection failed: %s " % e)

    @asyncio.coroutine
    def disconnect(self):
        try:
            self.machine.disconnect()
            disconnect_packet = DisconnectPacket()
            self.logger.debug(" -out-> " + repr(disconnect_packet))
            yield from disconnect_packet.to_stream(self._session.writer)
            self._session.writer.close()
        except MachineError as me:
            self.logger.debug("Invalid method call at this moment: %s" % me)
            raise ClientException("Client instance can't be disconnected: %s" % me)
        self._loop.stop()
        self._session = None

    @asyncio.coroutine
    def ping(self):
        ping_packet = PingReqPacket()
        self.logger.debug(" -out-> " + repr(ping_packet))
        yield from ping_packet.to_stream(self._session.writer)
        response = yield from PingRespPacket.from_stream(self._session.reader)
        self.logger.debug(" <-in-- " + repr(response))
        self._keep_alive()

    def _keep_alive(self):
        if self._ping_handle:
            try:
                self._ping_handle.cancel()
                self.logger.debug('Cancel pending ping')
            except Exception:
                pass
        next_ping = self._session.keep_alive-self.config['ping_delay']
        if next_ping > 0:
            self.logger.debug('Next ping in %d seconds' % next_ping)
            self._ping_handle = self._loop.call_later(next_ping, asyncio.async, self.ping())

    @asyncio.coroutine
    def publish(self, topic, message, dup=False, qos=None, retain=None):
        def get_retain_and_qos():
            if qos:
                _qos = qos
            else:
                _qos = self.config['default_qos']
                try:
                    _qos = self.config['topics'][topic]['qos']
                except KeyError:
                    pass
            if retain:
                _retain = retain
            else:
                _retain = self.config['default_retain']
                try:
                    _retain = self.config['topics'][topic]['retain']
                except KeyError:
                    pass
            return _qos, _retain
        (app_qos, app_retain) = get_retain_and_qos()
        if app_qos == 0:
            yield from self._publish_qos_0(topic, message, dup, app_retain)
        if app_qos == 1:
            yield from self._publish_qos_1(topic, message, dup, app_retain)
        if app_qos == 2:
            yield from self._publish_qos_2(topic, message, dup, app_retain)

    @asyncio.coroutine
    def _publish_qos_0(self, topic, message, dup, retain):
        packet = PublishPacket.build(topic, message, self._session.next_packet_id, dup, 0x00, retain)
        self.logger.debug(" -out-> " + repr(packet))
        yield from packet.to_stream(self._session.writer)
        self._keep_alive()

    @asyncio.coroutine
    def _publish_qos_1(self, topic, message, dup, retain):
        packet = PublishPacket.build(topic, message, self._session.next_packet_id, dup, 0x01, retain)
        self.logger.debug(" -out-> " + repr(packet))
        yield from packet.to_stream(self._session.writer)

        puback = yield from PubackPacket.from_stream(self._session.reader)
        self.logger.debug(" <-in-- " + repr(puback))
        self._keep_alive()

        if packet.variable_header.packet_id != puback.variable_header.packet_id:
            raise MQTTException("[MQTT-4.3.2-2] Puback packet packet_id doesn't match publish packet")

    @asyncio.coroutine
    def _publish_qos_2(self, topic, message, dup, retain):
        publish = PublishPacket.build(topic, message, self._session.next_packet_id, dup, 0x02, retain)
        self.logger.debug(" -out-> " + repr(publish))
        yield from publish.to_stream(self._session.writer)

        pubrec = yield from PubrecPacket.from_stream(self._session.reader)
        if publish.variable_header.packet_id != pubrec.variable_header.packet_id:
            raise MQTTException("[MQTT-4.3.2-2] Puback packet packet_id doesn't match publish packet")
        self.logger.debug(" <-in-- " + repr(pubrec))

        pubrel = PubrelPacket.build(pubrec.variable_header.packet_id)
        yield from pubrel.to_stream(self._session.writer)
        self.logger.debug(" -out-> " + repr(pubrel))

        pubcomp = yield from PubcompPacket.from_stream(self._session.reader)
        self.logger.debug(" <-in-- " + repr(pubcomp))
        if pubrel.variable_header.packet_id != pubcomp.variable_header.packet_id:
            raise MQTTException("[MQTT-4.3.2-2] Pubcomp packet packet_id doesn't match pubrel packet")
        self._keep_alive()

    @asyncio.coroutine
    def subscribe(self, topics):
        subscribe = SubscribePacket.build(topics, self._session.next_packet_id)
        yield from subscribe.to_stream(self._session.writer)
        self.logger.debug(" -out-> " + repr(subscribe))

        suback = yield from SubackPacket.from_stream(self._session.reader)
        self.logger.debug(" <-in-- " + repr(suback))
        if suback.variable_header.packet_id != subscribe.variable_header.packet_id:
            raise MQTTException("[MQTT-4.3.2-2] Suback packet packet_id doesn't match subscribe packet")
        self._keep_alive()

    @asyncio.coroutine
    def _connect_coro(self):
        try:
            self._session.reader, self._session.writer = \
                yield from asyncio.open_connection(self._session.remote_address, self._session.remote_port)
            self._session.local_address, self._session.local_port = self._session.writer.get_extra_info('sockname')

            # Send CONNECT packet and wait for CONNACK
            packet = ConnectPacket.build_request_from_session(self._session)
            yield from packet.to_stream(self._session.writer)
            self.logger.debug(" -out-> " + repr(packet))

            connack = yield from ConnackPacket.from_stream(self._session.reader)
            self.logger.debug(" <-in-- " + repr(connack))
            if connack.variable_header.return_code is not ReturnCode.CONNECTION_ACCEPTED:
                raise ClientException("Connection rejected with code '%s'" % hex(connack.variable_header.return_code))

            self._session.state = SessionState.CONNECTED
            self.logger.debug("connected to %s:%s" % (self._session.remote_address, self._session.remote_port))
        except Exception as e:
            self._session.state = SessionState.DISCONNECTED
            raise e

    def _init_session(self, host=None, port=None, username=None, password=None, uri=None, clean_session=None) -> dict:
        # Load config
        broker_conf = self.config.get('broker', dict()).copy()
        if 'mqtt' not in broker_conf:
            broker_conf['scheme'] = 'mqtt'
        if 'username' not in broker_conf:
            broker_conf['username'] = None
        if 'password' not in broker_conf:
            broker_conf['password'] = None

        if uri is not None:
            result = urlparse(uri)
            if result.scheme:
                broker_conf['scheme'] = result.scheme
            if result.hostname:
                broker_conf['host'] = result.hostname
            if result.port:
                broker_conf['port'] = result.port
            if result.username:
                broker_conf['username'] = result.username
            if result.password:
                broker_conf['password'] = result.password
        if host:
            broker_conf['host'] = host
        if port:
            broker_conf['port'] = int(port)
        if username:
            broker_conf['username'] = username
        if password:
            broker_conf['password'] = password
        if clean_session is not None:
            broker_conf['clean_session'] = clean_session

        for key in ['scheme', 'host', 'port']:
            if not_in_dict_or_none(broker_conf, key):
                raise ClientException("Missing connection parameter '%s'" % key)

        s = Session()
        s.client_id = self.client_id
        s.remote_address = broker_conf['host']
        s.remote_port = broker_conf['port']
        s.username = broker_conf['username']
        s.password = broker_conf['password']
        s.scheme = broker_conf['scheme']
        if clean_session is not None:
            s.clean_session = clean_session
        else:
            s.clean_session = self.config.get('clean_session', True)
        s.keep_alive = self.config['keep_alive']
        if 'will' in self.config:
            s.will_flag = True
            s.will_retain = self.config['will']['retain']
            s.will_topic = self.config['will']['topic']
            s.will_message = self.config['will']['message']
        else:
            s.will_flag = False
            s.will_retain = False
            s.will_topic = None
            s.will_message = None
        return s

