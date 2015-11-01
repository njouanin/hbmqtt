# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

import logging
import ssl
from urllib.parse import urlparse, urlunparse

from hbmqtt.utils import not_in_dict_or_none
from hbmqtt.session import Session
from hbmqtt.mqtt.connack import *
from hbmqtt.mqtt.connect import *
from hbmqtt.mqtt.protocol.client_handler import ClientProtocolHandler
from hbmqtt.adapters import StreamReaderAdapter, StreamWriterAdapter, WebSocketsReader, WebSocketsWriter
from hbmqtt.plugins.manager import PluginManager, BaseContext
from hbmqtt.mqtt.protocol.handler import EVENT_MQTT_PACKET_SENT, EVENT_MQTT_PACKET_RECEIVED, ProtocolHandlerException
from hbmqtt.mqtt.constants import *
import websockets
from websockets.uri import InvalidURI
from websockets.handshake import InvalidHandshake
from collections import deque
import sys
if sys.version_info < (3, 5):
    from asyncio import async as ensure_future
else:
    from asyncio import ensure_future

_defaults = {
    'keep_alive': 10,
    'ping_delay': 1,
    'default_qos': 0,
    'default_retain': False,
    'auto_reconnect': True,
    'reconnect_max_interval': 10,
    'reconnect_retries': 2,
}


class ClientException(BaseException):
    pass


class ConnectException(ClientException):
    pass


class ClientContext(BaseContext):
    """
    ClientContext is used as the context passed to plugins interacting with the client.
    It act as an adapter to client services from plugins
    """
    def __init__(self):
        super().__init__()
        self.config = None

base_logger = logging.getLogger(__name__)


def mqtt_connected(func):
    """
    MQTTClient coroutines decorator which will wait until connection before calling the decorated method.
    :param func: coroutine to be called once connected
    :return: coroutine result
    """
    @asyncio.coroutine
    def wrapper(self, *args, **kwargs):
        if not self._connected_state.is_set():
            base_logger.warning("Client not connected, waiting for it")
            yield from self._connected_state.wait()
        return (yield from func(self, *args, **kwargs))
    return wrapper


class MQTTClient:
    def __init__(self, client_id=None, config=None, loop=None):
        """

        :param config: Example yaml config
            broker:
                uri: mqtt:username@password//localhost:1883/
                cafile: somefile.cert  #Server authority file
                capath: /some/path # certficate file path
                cadata: certificate as string data
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
        if config is not None:
            self.config.update(config)
        if client_id is not None:
            self.client_id = client_id
        else:
            from hbmqtt.utils import gen_client_id
            self.client_id = gen_client_id()
            self.logger.debug("Using generated client ID : %s" % self.client_id)

        if loop is not None:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
        self.session = None
        self._handler = None
        self._disconnect_task = None
        self._connected_state = asyncio.Event()

        # Init plugins manager
        context = ClientContext()
        context.config = self.config
        self.plugins_manager = PluginManager('hbmqtt.client.plugins', context)
        self.client_tasks = deque()


    @asyncio.coroutine
    def connect(self,
                uri=None,
                cleansession=None,
                cafile=None,
                capath=None,
                cadata=None):
        """
        Connect to a remote broker
        :param uri: Broker URI connection, conforming to `MQTT URI scheme <https://github.com/mqtt/mqtt.github.io/wiki/URI-Scheme>`_.
        :param cleansession: MQTT CONNECT clean session flaf
        :param cafile: server certificate authority file
        :return:
        """
        self.session = self._initsession(uri, cleansession, cafile, capath, cadata)
        self.logger.debug("Connect to: %s" % uri)

        try:
            return (yield from self._do_connect())
        except BaseException as be:
            self.logger.warning("Connection failed: %r" % be)
            auto_reconnect = self.config.get('auto_reconnect', False)
            if not auto_reconnect:
                raise
            else:
                return (yield from self.reconnect())

    @mqtt_connected
    @asyncio.coroutine
    def disconnect(self):
        if self.session.transitions.is_connected():
            if not self._disconnect_task.done():
                self._disconnect_task.cancel()
            yield from self._handler.mqtt_disconnect()
            self._connected_state.clear()
            yield from self._handler.stop()
            self.session.transitions.disconnect()
        else:
            self.logger.warn("Client session is not currently connected, ignoring call")

    @asyncio.coroutine
    def reconnect(self, cleansession=None):
        if self.session.transitions.is_connected():
            self.logger.warn("Client already connected")
            return CONNECTION_ACCEPTED

        if cleansession:
            self.session.clean_session = cleansession
        self.logger.debug("Reconnecting with session parameters: %s" % self.session)
        reconnect_max_interval = self.config.get('reconnect_max_interval', 10)
        reconnect_retries = self.config.get('reconnect_retries', 5)
        nb_attempt = 1
        yield from asyncio.sleep(1, loop=self._loop)
        while True:
            try:
                self.logger.debug("Reconnect attempt %d ..." % nb_attempt)
                return (yield from self._do_connect())
            except BaseException as e:
                self.logger.warning("Reconnection attempt failed: %r" % e)
                if nb_attempt > reconnect_retries:
                    self.logger.error("Maximum number of connection attempts reached. Reconnection aborted")
                    raise ConnectException("Too many connection attempts failed")
                exp = 2 ** nb_attempt
                delay = exp if exp < reconnect_max_interval else reconnect_max_interval
                self.logger.debug("Waiting %d second before next attempt" % delay)
                yield from asyncio.sleep(delay, loop=self._loop)
                nb_attempt += 1


    @asyncio.coroutine
    def _do_connect(self):
        return_code = yield from self._connect_coro()
        self._disconnect_task = ensure_future(self.handle_connection_close(), loop=self._loop)
        return return_code

    @mqtt_connected
    @asyncio.coroutine
    def ping(self):
        """
        Send a MQTT ping request and wait for response
        :return: None
        """
        if self.session.transitions.is_connected():
            yield from self._handler.mqtt_ping()
        else:
            self.logger.warn("MQTT PING request incompatible with current session state '%s'" %
                             self.session.transitions.state)

    @mqtt_connected
    @asyncio.coroutine
    def publish(self, topic, message, qos=None, retain=None):
        def get_retain_and_qos():
            if qos:
                assert qos in (QOS_0, QOS_1, QOS_2)
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
        return (yield from self._handler.mqtt_publish(topic, message, app_qos, app_retain))

    @mqtt_connected
    @asyncio.coroutine
    def subscribe(self, topics):
        return (yield from self._handler.mqtt_subscribe(topics, self.session.next_packet_id))

    @mqtt_connected
    @asyncio.coroutine
    def unsubscribe(self, topics):
        yield from self._handler.mqtt_unsubscribe(topics, self.session.next_packet_id)

    @asyncio.coroutine
    def deliver_message(self, timeout=None):
        deliver_task = ensure_future(self._handler.mqtt_deliver_next_message(), loop=self._loop)
        self.client_tasks.append(deliver_task)
        self.logger.debug("Waiting message delivery")
        yield from asyncio.wait([deliver_task], loop=self._loop, return_when=asyncio.FIRST_EXCEPTION, timeout=timeout)
        if deliver_task.exception():
            raise deliver_task.exception()
        self.client_tasks.pop()
        return deliver_task.result()

    @asyncio.coroutine
    def _connect_coro(self):
        kwargs = dict()

        # Decode URI attributes
        uri_attributes = urlparse(self.session.broker_uri)
        scheme = uri_attributes.scheme
        secure = True if scheme in ('mqtts', 'wss') else False        
        self.session.username = uri_attributes.username
        self.session.password = uri_attributes.password
        self.session.remote_address = uri_attributes.hostname
        self.session.remote_port = uri_attributes.port
        if scheme in ('mqtt', 'mqtts') and not self.session.remote_port:
            self.session.remote_port = 8883 if scheme == 'mqtts' else 1883
        if scheme in ('ws', 'wss') and not self.session.remote_port:
            self.session.remote_port = 443 if scheme == 'wss' else 80
        if scheme in ('ws', 'wss'):
            # Rewrite URI to conform to https://tools.ietf.org/html/rfc6455#section-3
            uri = (scheme, self.session.remote_address + ":" + str(self.session.remote_port), uri_attributes[2],
                   uri_attributes[3], uri_attributes[4], uri_attributes[5])
            self.session.broker_uri = urlunparse(uri)
        # Init protocol handler
        #if not self._handler:
        self._handler = ClientProtocolHandler(self.plugins_manager, loop=self._loop)

        if secure:
            if self.session.cafile is None or self.session.cafile == '':
                self.logger.warn("TLS connection can't be estabilshed, no certificate file (.cert) given")
                raise ClientException("TLS connection can't be estabilshed, no certificate file (.cert) given")
            sc = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH,
                cafile=self.session.cafile,
                capath=self.session.capath,
                cadata=self.session.cadata)
            if 'certfile' in self.config and 'keyfile' in self.config:
                sc.load_cert_chain(self.config['certfile'], self.config['keyfile'])
            kwargs['ssl'] = sc

        try:
            reader = None
            writer = None
            self._connected_state.clear()
            # Open connection
            if scheme in ('mqtt', 'mqtts'):
                conn_reader, conn_writer = \
                    yield from asyncio.open_connection(
                        self.session.remote_address,
                        self.session.remote_port, loop=self._loop, **kwargs)
                reader = StreamReaderAdapter(conn_reader)
                writer = StreamWriterAdapter(conn_writer)
            elif scheme in ('ws', 'wss'):
                websocket = yield from websockets.connect(
                    self.session.broker_uri,
                    subprotocols=['mqtt'],
                    loop=self._loop,
                    **kwargs)
                reader = WebSocketsReader(websocket)
                writer = WebSocketsWriter(websocket)
            # Start MQTT protocol
            self._handler.attach(self.session, reader, writer)
            return_code = yield from self._handler.mqtt_connect()
            if return_code is not CONNECTION_ACCEPTED:
                self.session.transitions.disconnect()
                self.logger.warning("Connection rejected with code '%s'" % return_code)
                exc = ConnectException("Connection rejected by broker")
                exc.return_code = return_code
                raise exc
            else:
                # Handle MQTT protocol
                yield from self._handler.start()
                self.session.transitions.connect()
                self._connected_state.set()
                self.logger.debug("connected to %s:%s" % (self.session.remote_address, self.session.remote_port))
            return return_code
        except InvalidURI as iuri:
            self.logger.warn("connection failed: invalid URI '%s'" % self.session.broker_uri)
            self.session.transitions.disconnect()
            raise ConnectException("connection failed: invalid URI '%s'" % self.session.broker_uri, iuri)
        except InvalidHandshake as ihs:
            self.logger.warn("connection failed: invalid websocket handshake")
            self.session.transitions.disconnect()
            raise ConnectException("connection failed: invalid websocket handshake", ihs)
        except (ProtocolHandlerException, ConnectionError, OSError) as e:
            self.logger.warn("MQTT connection failed: %r" % e)
            self.session.transitions.disconnect()
            raise ConnectException(e)

    @asyncio.coroutine
    def handle_connection_close(self):
        self.logger.debug("Watch broker disconnection")
        # Wait for disconnection from broker (like connection lost)
        yield from self._handler.wait_disconnect()
        self.logger.warning("Disconnected from broker")

        # Block client API
        self._connected_state.clear()

        # stop an clean handler
        yield from self._handler.stop()
        self._handler.detach()
        self.session.transitions.disconnect()

        if self.config.get('auto_reconnect', False):
            # Try reconnection
            self.logger.debug("Auto-reconnecting")
            try:
                yield from self.reconnect()
            except ConnectException:
                # Cancel client pending tasks
                while self.client_tasks:
                    self.client_tasks.popleft().set_exception(ClientException("Connection lost"))
        else:
            # Cancel client pending tasks
            while self.client_tasks:
                self.client_tasks.popleft().set_exception(ClientException("Connection lost"))

    def _initsession(
            self,
            uri=None,
            cleansession=None,
            cafile=None,
            capath=None,
            cadata=None) -> Session:
        # Load config
        broker_conf = self.config.get('broker', dict()).copy()
        if uri:
            broker_conf['uri'] = uri
        if cafile:
            broker_conf['cafile'] = cafile
        elif 'cafile' not in broker_conf:
            broker_conf['cafile'] = None
        if capath:
            broker_conf['capath'] = capath
        elif 'capath' not in broker_conf:
            broker_conf['capath'] = None
        if cadata:
            broker_conf['cadata'] = cadata
        elif 'cadata' not in broker_conf:
            broker_conf['cadata'] = None

        if cleansession is not None:
            broker_conf['cleansession'] = cleansession

        for key in ['uri']:
            if not_in_dict_or_none(broker_conf, key):
                raise ClientException("Missing connection parameter '%s'" % key)

        s = Session()
        s.broker_uri = uri
        s.client_id = self.client_id
        s.cafile = broker_conf['cafile']
        s.capath = broker_conf['capath']
        s.cadata = broker_conf['cadata']
        if cleansession is not None:
            s.clean_session = cleansession
        else:
            s.clean_session = self.config.get('cleansession', True)
        s.keep_alive = self.config['keep_alive'] - self.config['ping_delay']
        if 'will' in self.config:
            s.will_flag = True
            s.will_retain = self.config['will']['retain']
            s.will_topic = self.config['will']['topic']
            s.will_message = self.config['will']['message']
            s.will_qos = self.config['will']['qos']
        else:
            s.will_flag = False
            s.will_retain = False
            s.will_topic = None
            s.will_message = None
        return s
