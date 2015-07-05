__author__ = 'nico'

import logging
import asyncio
from urllib.parse import urlparse

from transitions import Machine, MachineError

from hbmqtt.utils import not_in_dict_or_none
from hbmqtt.session import Session, SessionState
from hbmqtt.mqtt.connack import ReturnCode
from hbmqtt.mqtt.subscribe import SubscribePacket
from hbmqtt.mqtt.suback import SubackPacket
from hbmqtt.errors import MQTTException
from hbmqtt.mqtt.protocol import ClientProtocolHandler

_defaults = {
    'keep_alive': 10,
    'ping_delay': 1,
    'default_qos': 0,
    'default_retain': False,
    'inflight-polling-interval': 1,
    'subscriptions-polling-interval': 1,
}


class ClientException(BaseException):
    pass


def gen_client_id():
    import uuid
    return str(uuid.uuid4())


class MQTTClient:
    states = ['new', 'connecting', 'connected', 'disconnected']

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
            cleansession: true
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
        self.config = _defaults
        self.config.update(config)
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
        self.session = None
        self._handler = None

    def _init_states(self):
        self.machine = Machine(states=MQTTClient.states, initial='new')
        self.machine.add_transition(trigger='connect', source='new', dest='connecting')
        self.machine.add_transition(trigger='connect', source='disconnected', dest='connecting')
        self.machine.add_transition(trigger='connect_fail', source='connecting', dest='disconnected')
        self.machine.add_transition(trigger='connect_success', source='connecting', dest='connected')
        self.machine.add_transition(trigger='disconnect', source='idle', dest='disconnected')
        self.machine.add_transition(trigger='disconnect', source='connected', dest='disconnected')

    @asyncio.coroutine
    def connect(self, host=None, port=None, username=None, password=None, uri=None, cleansession=None):
        try:
            self.machine.connect()
            self.session = self._initsession(host, port, username, password, uri, cleansession)
            self.logger.debug("Connect with session parameters: %s" % self.session)

            return_code = yield from self._connect_coro()
            self.machine.connect_success()
            return return_code
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
            yield from self._handler.mqtt_disconnect()
            yield from self._handler.stop()
        except Exception as e:
            self.logger.warn("Unhandled exception: %s" % e)
            raise ClientException("Unhandled exception: %s" % e)
        except MachineError as me:
            self.logger.debug("Invalid method call at this moment: %s" % me)
            raise ClientException("Client instance can't be disconnected: %s" % me)
        self.session = None

    @asyncio.coroutine
    def ping(self):
        """
        Send a MQTT ping request and wait for response
        :return: None
        """
        self._handler.mqtt_ping()

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
        yield from self._handler.mqtt_publish(topic, message, self.session.next_packet_id, dup, 0x00, retain)

    @asyncio.coroutine
    def _publish_qos_1(self, topic, message, dup, retain):
        yield from self._handler.mqtt_publish(topic, message, self.session.next_packet_id, dup, 0x01, retain)

    @asyncio.coroutine
    def _publish_qos_2(self, topic, message, dup, retain):
        yield from self._handler.mqtt_publish(topic, message, self.session.next_packet_id, dup, 0x02, retain)

    @asyncio.coroutine
    def subscribe(self, topics):
        return (yield from self._handler.mqtt_subscribe(topics, self.session.next_packet_id))

    @asyncio.coroutine
    def unsubscribe(self, topics):
        yield from self._handler.mqtt_unsubscribe(topics, self.session.next_packet_id)

    @asyncio.coroutine
    def _connect_coro(self):
        try:
            self.session.reader, self.session.writer = \
                yield from asyncio.open_connection(self.session.remote_address, self.session.remote_port)
            self._handler = ClientProtocolHandler(self.session, self.config)
            yield from self._handler.start()

            return_code = yield from self._handler.mqtt_connect()

            if return_code is not ReturnCode.CONNECTION_ACCEPTED:
                raise ClientException("Connection rejected with code '%s'" % hex(return_code))

            self.session.state = SessionState.CONNECTED
            self.logger.debug("connected to %s:%s" % (self.session.remote_address, self.session.remote_port))
            return return_code
        except Exception as e:
            self.session.state = SessionState.DISCONNECTED
            raise e

    def _initsession(self, host=None, port=None, username=None, password=None, uri=None, cleansession=None) -> dict:
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
        if cleansession is not None:
            broker_conf['cleansession'] = cleansession

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
        if cleansession is not None:
            s.cleansession = cleansession
        else:
            s.cleansession = self.config.get('cleansession', True)
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

