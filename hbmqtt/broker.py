# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging
import ssl
import websockets
import asyncio
from datetime import datetime

from functools import partial
from transitions import Machine, MachineError
from hbmqtt.session import Session
from hbmqtt.mqtt.protocol.broker_handler import BrokerProtocolHandler
from hbmqtt.mqtt.connack import *
from hbmqtt.errors import HBMQTTException, MQTTException
from hbmqtt.utils import format_client_message, gen_client_id
from hbmqtt.mqtt.packet import PUBLISH
from hbmqtt.codecs import int_to_bytes_str
from hbmqtt.adapters import (
    StreamReaderAdapter,
    StreamWriterAdapter,
    ReaderAdapter,
    WriterAdapter,
    WebSocketsReader,
    WebSocketsWriter)
from .plugins.manager import PluginManager, BaseContext

import sys
if sys.version_info < (3, 5):
    from asyncio import async as ensure_future


_defaults = {
    'timeout-disconnect-delay': 2,
    'publish-retry-delay': 5,
    'auth': {
        'allow-anonymous': True,
        'password-file': None
    }
}

DOLLAR_SYS_ROOT = '$SYS/broker/'
STAT_BYTES_SENT = 'bytes_sent'
STAT_BYTES_RECEIVED = 'bytes_received'
STAT_MSG_SENT = 'messages_sent'
STAT_MSG_RECEIVED = 'messages_received'
STAT_PUBLISH_SENT = 'publish_sent'
STAT_PUBLISH_RECEIVED = 'publish_received'
STAT_UPTIME = 'uptime'
STAT_CLIENTS_MAXIMUM = 'clients_maximum'

EVENT_BROKER_PRE_START = 'broker_pre_start'
EVENT_BROKER_POST_START = 'broker_post_start'
EVENT_BROKER_PRE_SHUTDOWN = 'broker_pre_shutdown'
EVENT_BROKER_POST_SHUTDOWN = 'broker_post_shutdown'
EVENT_BROKER_CLIENT_CONNECTED = 'broker_client_connected'
EVENT_BROKER_CLIENT_DISCONNECTED = 'broker_client_disconnected'
EVENT_BROKER_CLIENT_SUBSCRIBED = 'broker_client_subscribed'
EVENT_BROKER_CLIENT_UNSUBSCRIBED = 'broker_client_unsubscribed'


class BrokerException(BaseException):
    pass


class RetainedApplicationMessage:
    def __init__(self, source_session, topic, data, qos=None):
        self.source_session = source_session
        self.topic = topic
        self.data = data
        self.qos = qos


class Server:
    def __init__(self, listener_name, server_instance, max_connections=-1, loop=None):
        self.logger = logging.getLogger(__name__)
        self.instance = server_instance
        self.conn_count = 0
        self.listener_name = listener_name
        if loop is not None:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()

        self.max_connections = max_connections
        if self.max_connections > 0:
            self.semaphore = asyncio.Semaphore(self.max_connections, loop=self._loop)
        else:
            self.semaphore = None

    @asyncio.coroutine
    def acquire_connection(self):
        if self.semaphore:
            yield from self.semaphore.acquire()
        self.conn_count += 1
        if self.max_connections > 0:
            self.logger.debug("Listener '%s': %d/%d connections acquired" %
                              (self.listener_name, self.conn_count, self.max_connections))

    def release_connection(self):
        if self.semaphore:
            self.semaphore.release()
        self.conn_count -= 1
        if self.max_connections > 0:
            self.logger.debug("Listener '%s': %d/%d connections acquired" %
                              (self.listener_name, self.conn_count, self.max_connections))

    @asyncio.coroutine
    def close_instance(self):
        if self.instance:
            self.instance.close()
            yield from self.instance.wait_closed()


class BrokerContext(BaseContext):
    """
    BrokerContext is used as the context passed to plugins interacting with the broker.
    It act as an adapter to broker services from plugins developed for HBMQTT broker
    """
    def __init__(self):
        super().__init__()
        self.config = None


class Broker:
    states = ['new', 'starting', 'started', 'not_started', 'stopping', 'stopped', 'not_stopped', 'stopped']

    def __init__(self, config=None, loop=None, plugin_namespace=None):
        """

        :param config: Example Yaml config
            listeners:
                default:  #Mandatory
                    max-connections: 50000
                    type: tcp
                my-tcp-1:
                    bind: 127.0.0.1:1883
                my-tcp-2:
                    bind: 1.2.3.4:1883
                    max-connections: 1000
                my-tcp-ssl-1:
                    bind: 127.0.0.1:8883
                    ssl: on
                    cafile: /some/cafile
                    capath: /some/folder
                    capath: certificate data
                    certfile: /some/certfile
                    keyfile: /some/key
                my-ws-1:
                    bind: 0.0.0.0:8080
                    type: ws
            timeout-disconnect-delay: 2
            publish-retry-delay: 5
            auth:
                plugins: ['auth.anonymous'] #List of plugins to activate for authentication among all registered plugins
                allow-anonymous: true / false
                password-file: /some/passwd_file
            persistence:
                plugin: 'persistence-sqlite'

        :param loop:
        :return:
        """
        self.logger = logging.getLogger(__name__)
        self.config = _defaults
        if config is not None:
            self.config.update(config)
        self._build_listeners_config(self.config)

        if loop is not None:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()

        self._servers = dict()
        self._init_states()
        self._sessions = dict()
        self._subscriptions = dict()
        self._global_retained_messages = dict()

        # Broker statistics initialization
        self._stats = dict()

        # $SYS tree task handle
        self.sys_handle = None

        # Init plugins manager
        context = BrokerContext()
        context.config = self.config
        if plugin_namespace:
            namespace = plugin_namespace
        else:
            namespace = 'hbmqtt.broker.plugins'
        self.plugins_manager = PluginManager(namespace, context, self._loop)

    def _build_listeners_config(self, broker_config):
        self.listeners_config = dict()
        try:
            listeners_config = broker_config['listeners']
            defaults = listeners_config['default']
            for listener in listeners_config:
                config = dict(defaults)
                config.update(listeners_config[listener])
                self.listeners_config[listener] = config
        except KeyError as ke:
            raise BrokerException("Listener config not found invalid: %s" % ke)

    def _init_states(self):
        self.transitions = Machine(states=Broker.states, initial='new')
        self.transitions.add_transition(trigger='start', source='new', dest='starting')
        self.transitions.add_transition(trigger='starting_fail', source='starting', dest='not_started')
        self.transitions.add_transition(trigger='starting_success', source='starting', dest='started')
        self.transitions.add_transition(trigger='shutdown', source='started', dest='stopping')
        self.transitions.add_transition(trigger='stopping_success', source='stopping', dest='stopped')
        self.transitions.add_transition(trigger='stopping_failure', source='stopping', dest='not_stopped')
        self.transitions.add_transition(trigger='start', source='stopped', dest='starting')

    @asyncio.coroutine
    def start(self):
        try:
            self._sessions = dict()
            self._subscriptions = dict()
            self._global_retained_messages = dict()
            self.transitions.start()
            self.logger.debug("Broker starting")
        except MachineError as me:
            self.logger.warn("[WARN-0001] Invalid method call at this moment: %s" % me)
            raise BrokerException("Broker instance can't be started: %s" % me)

        yield from self.plugins_manager.fire_event(EVENT_BROKER_PRE_START)
        # Clear broker stats
        self._clear_stats()
        try:
            # Start network listeners
            for listener_name in self.listeners_config:
                listener = self.listeners_config[listener_name]

                # Max connections
                try:
                    max_connections = listener['max_connections']
                except KeyError:
                    max_connections = -1

                # SSL Context
                sc = None
                if 'ssl' in listener and listener['ssl'].upper() == 'ON':
                    try:
                        sc = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                        sc.load_cert_chain(listener['certfile'], listener['keyfile'])
                        sc.verify_mode = ssl.CERT_OPTIONAL
                    except KeyError as ke:
                        raise BrokerException("'certfile' or 'keyfile' configuration parameter missing: %s" % ke)
                    except FileNotFoundError as fnfe:
                        raise BrokerException("Can't read cert files '%s' or '%s' : %s" %
                                              (listener['certfile'], listener['keyfile'], fnfe))

                if listener['type'] == 'tcp':
                    address, port = listener['bind'].split(':')
                    cb_partial = partial(self.stream_connected, listener_name=listener_name)
                    instance = yield from asyncio.start_server(cb_partial,
                                                               address,
                                                               port,
                                                               ssl=sc,
                                                               loop=self._loop)
                    self._servers[listener_name] = Server(listener_name, instance, max_connections, self._loop)
                elif listener['type'] == 'ws':
                    address, port = listener['bind'].split(':')
                    cb_partial = partial(self.ws_connected, listener_name=listener_name)
                    instance = yield from websockets.serve(cb_partial, address, port, ssl=sc, loop=self._loop)
                    self._servers[listener_name] = Server(listener_name, instance, max_connections, self._loop)

                self.logger.info("Listener '%s' bind to %s" % (listener_name, listener['bind']))

            # Start $SYS topics management
            try:
                sys_interval = int(self.config.get('sys_interval', 0))
                if sys_interval > 0:
                    self.logger.debug("Setup $SYS broadcasting every %d secondes" % sys_interval)
                    self._init_dollar_sys()
                    self.sys_handle = self._loop.call_later(sys_interval, self.broadcast_dollar_sys_topics)
            except KeyError:
                pass
                # 'sys_internal' config parameter not found

            self.transitions.starting_success()
            yield from self.plugins_manager.fire_event(EVENT_BROKER_POST_START)
            self.logger.debug("Broker started")
        except Exception as e:
            self.logger.error("Broker startup failed: %s" % e)
            self.transitions.starting_fail()
            raise BrokerException("Broker instance can't be started: %s" % e)

    @asyncio.coroutine
    def shutdown(self):
        try:
            self._sessions = dict()
            self._subscriptions = dict()
            self._global_retained_messages = dict()
            self.transitions.shutdown()
        except MachineError as me:
            self.logger.debug("Invalid method call at this moment: %s" % me)
            raise BrokerException("Broker instance can't be stopped: %s" % me)

        # Fire broker_shutdown event to plugins
        yield from self.plugins_manager.fire_event(EVENT_BROKER_PRE_SHUTDOWN)

        # Stop $SYS topics broadcasting
        if self.sys_handle:
            self.sys_handle.cancel()

        for listener_name in self._servers:
            server = self._servers[listener_name]
            yield from server.close_instance()
        self.logger.debug("Broker closing")
        self.logger.info("Broker closed")
        yield from self.plugins_manager.fire_event(EVENT_BROKER_POST_SHUTDOWN)
        self.transitions.stopping_success()

    def _clear_stats(self):
        """
        Initializes broker statistics data structures
        """
        for stat in (STAT_BYTES_RECEIVED,
                     STAT_BYTES_SENT,
                     STAT_MSG_RECEIVED,
                     STAT_MSG_SENT,
                     STAT_CLIENTS_MAXIMUM,
                     STAT_PUBLISH_RECEIVED,
                     STAT_PUBLISH_SENT):
            self._stats[stat] = 0
        self._stats[STAT_UPTIME] = datetime.now()

    def _init_dollar_sys(self):
        """
        Initializes and publish $SYS static topics
        """
        from hbmqtt.version import get_version
        self.sys_handle = None
        version = 'HBMQTT version ' + get_version()
        self.retain_message(None, DOLLAR_SYS_ROOT + 'version', version.encode())

    def broadcast_dollar_sys_topics(self):
        """
        Broadcast dynamic $SYS topics updates and reschedule next execution depending on 'sys_interval' config
        parameter.
        """

        # Update stats
        uptime = datetime.now() - self._stats[STAT_UPTIME]
        client_connected = sum(1 for k, session in self._sessions.items() if session.transitions.state == 'connected')
        if client_connected > self._stats[STAT_CLIENTS_MAXIMUM]:
            self._stats[STAT_CLIENTS_MAXIMUM] = client_connected
        client_disconnected = sum(1 for k, session in self._sessions.items() if session.transitions.state == 'disconnected')
        inflight_in = 0
        inflight_out = 0
        messages_stored = 0
        for k, session in self._sessions.items():
            inflight_in += session.inflight_in_count
            inflight_out += session.inflight_out_count
            messages_stored += session.retained_messages_count
        messages_stored += len(self._global_retained_messages)
        subscriptions_count = 0
        for topic in self._subscriptions:
            subscriptions_count += len(self._subscriptions[topic])

        # Broadcast updates
        tasks = [
            self._broadcast_sys_topic('load/bytes/received', int_to_bytes_str(self._stats[STAT_BYTES_RECEIVED])),
            self._broadcast_sys_topic('load/bytes/sent', int_to_bytes_str(self._stats[STAT_BYTES_SENT])),
            self._broadcast_sys_topic('messages/received', int_to_bytes_str(self._stats[STAT_MSG_RECEIVED])),
            self._broadcast_sys_topic('messages/sent', int_to_bytes_str(self._stats[STAT_MSG_SENT])),
            self._broadcast_sys_topic('time', str(datetime.now()).encode('utf-8')),
            self._broadcast_sys_topic('uptime', int_to_bytes_str(int(uptime.total_seconds()))),
            self._broadcast_sys_topic('uptime/formated', str(uptime).encode('utf-8')),
            self._broadcast_sys_topic('clients/connected', int_to_bytes_str(client_connected)),
            self._broadcast_sys_topic('clients/disconnected', int_to_bytes_str(client_disconnected)),
            self._broadcast_sys_topic('clients/maximum', int_to_bytes_str(self._stats[STAT_CLIENTS_MAXIMUM])),
            self._broadcast_sys_topic('clients/total', int_to_bytes_str(client_connected + client_disconnected)),
            self._broadcast_sys_topic('messages/inflight', int_to_bytes_str(inflight_in + inflight_out)),
            self._broadcast_sys_topic('messages/inflight/in', int_to_bytes_str(inflight_in)),
            self._broadcast_sys_topic('messages/inflight/out', int_to_bytes_str(inflight_out)),
            self._broadcast_sys_topic('messages/inflight/stored', int_to_bytes_str(messages_stored)),
            self._broadcast_sys_topic('messages/publish/received', int_to_bytes_str(self._stats[STAT_PUBLISH_RECEIVED])),
            self._broadcast_sys_topic('messages/publish/sent', int_to_bytes_str(self._stats[STAT_PUBLISH_SENT])),
            self._broadcast_sys_topic('messages/retained/count', int_to_bytes_str(len(self._global_retained_messages))),
            self._broadcast_sys_topic('messages/subscriptions/count', int_to_bytes_str(subscriptions_count)),
        ]

        # Wait until broadcasting tasks end
        if len(tasks) > 0:
            yield from asyncio.wait(tasks, loop=self._loop)
        # Reschedule
        sys_interval = int(self.config['sys_interval'])
        self.logger.debug("Broadcasting $SYS topics")
        self.sys_handle = self._loop.call_later(sys_interval, self.broadcast_dollar_sys_topics)

    def _broadcast_sys_topic(self, topic_basename, data):
        return self._internal_message_broadcast(DOLLAR_SYS_ROOT + topic_basename, data)

    def _internal_message_broadcast(self, topic, data):
        return asyncio.Task(self.broadcast_application_message(None, topic, data), loop=self._loop)

    @asyncio.coroutine
    def ws_connected(self, websocket, uri, listener_name):
        yield from self.client_connected(listener_name, WebSocketsReader(websocket), WebSocketsWriter(websocket))

    @asyncio.coroutine
    def stream_connected(self, reader, writer, listener_name):
        yield from self.client_connected(listener_name, StreamReaderAdapter(reader), StreamWriterAdapter(writer))

    @asyncio.coroutine
    def client_connected(self, listener_name, reader: ReaderAdapter, writer: WriterAdapter):
        # Wait for connection available on listener
        server = self._servers.get(listener_name, None)
        if not server:
            raise BrokerException("Invalid listener name '%s'" % listener_name)
        yield from server.acquire_connection()

        remote_address, remote_port = writer.get_peer_info()
        self.logger.debug("Connection from %s:%d on listener '%s'" % (remote_address, remote_port, listener_name))

        # Wait for first packet and expect a CONNECT
        try:
            handler, client_session = yield from BrokerProtocolHandler.init_from_connect(reader, writer, self.plugins_manager)
        except HBMQTTException as exc:
            self.logger.warn("[MQTT-3.1.0-1] %s: Can't read first packet an CONNECT: %s" %
                             (format_client_message(address=remote_address, port=remote_port), exc))
            yield from writer.close()
            self.logger.debug("Connection closed")
            return
        except MQTTException as me:
            self.logger.error('Invalid connection from %s : %s' %
                              (format_client_message(address=remote_address, port=remote_port), me))
            yield from writer.close()
            self.logger.debug("Connection closed")
            return

        if client_session.clean_session:
            # Delete existing session and create a new one
            if client_session.client_id is not None:
                self.delete_session(client_session.client_id)
            else:
                client_session.client_id = gen_client_id()
            client_session.parent = 0
        else:
            # Get session from cache
            if client_session.client_id in self._sessions:
                self.logger.debug("Found old session %s" % repr(self._sessions[client_session.client_id]))
                (client_session,) = self._sessions[client_session.client_id]
                client_session.parent = 1
            else:
                client_session.parent = 0
        if client_session.keep_alive > 0:
            client_session.keep_alive += self.config['timeout-disconnect-delay']
        self.logger.debug("Keep-alive timeout=%d" % client_session.keep_alive)
        client_session.publish_retry_delay = self.config['publish-retry-delay']

        handler.attach(client_session, reader, writer)
        self._sessions[client_session.client_id] = (client_session, handler)

        authenticated = yield from self.authenticate(client_session, self.listeners_config[listener_name])
        yield from handler.mqtt_connack_authorize(authenticated)
        if not authenticated:
            yield from writer.close()
            return

        client_session.transitions.connect()
        yield from self.plugins_manager.fire_event(EVENT_BROKER_CLIENT_CONNECTED, client_id=client_session.client_id)

        self.logger.debug("%s Start messages handling" % client_session.client_id)
        yield from handler.start()
        self.logger.debug("Retained messages queue size: %d" % client_session.retained_messages.qsize())
        yield from self.publish_session_retained_messages(client_session)

        # Init and start loop for handling client messages (publish, subscribe/unsubscribe, disconnect)
        connected = True
        disconnect_waiter = asyncio.ensure_future(handler.wait_disconnect(), loop=self._loop)
        subscribe_waiter = asyncio.ensure_future(handler.get_next_pending_subscription(), loop=self._loop)
        unsubscribe_waiter = asyncio.ensure_future(handler.get_next_pending_unsubscription(), loop=self._loop)
        wait_deliver = asyncio.ensure_future(handler.mqtt_deliver_next_message(), loop=self._loop)
        while connected:
            done, pending = yield from asyncio.wait(
                [disconnect_waiter, subscribe_waiter, unsubscribe_waiter, wait_deliver],
                return_when=asyncio.FIRST_COMPLETED, loop=self._loop)
            if disconnect_waiter in done:
                result = disconnect_waiter.result()
                self.logger.debug("%s Result from wait_diconnect: %s" % (client_session.client_id, result))
                if result is None:
                    self.logger.debug("Will flag: %s" % client_session.will_flag)
                    # Connection closed anormally, send will message
                    if client_session.will_flag:
                        self.logger.debug("Client %s disconnected abnormally, sending will message" %
                                          format_client_message(client_session))
                        yield from self.broadcast_application_message(
                            client_session, client_session.will_topic,
                            client_session.will_message,
                            client_session.will_qos)
                        if client_session.will_retain:
                            self.retain_message(client_session,
                                                client_session.will_topic,
                                                client_session.will_message,
                                                client_session.will_qos)
                connected = False
            if unsubscribe_waiter in done:
                self.logger.debug("%s handling unsubscription" % client_session.client_id)
                unsubscription = unsubscribe_waiter.result()
                for topic in unsubscription['topics']:
                    self.del_subscription(topic, client_session)
                    yield from self.plugins_manager.fire_event(
                        EVENT_BROKER_CLIENT_UNSUBSCRIBED,
                        client_id=client_session.client_id,
                        topic=topic)
                yield from handler.mqtt_acknowledge_unsubscription(unsubscription['packet_id'])
                unsubscribe_waiter = asyncio.Task(handler.get_next_pending_unsubscription(), loop=self._loop)
            if subscribe_waiter in done:
                self.logger.debug("%s handling subscription" % client_session.client_id)
                subscriptions = subscribe_waiter.result()
                return_codes = []
                for subscription in subscriptions['topics']:
                    return_codes.append(self.add_subscription(subscription, client_session))
                yield from handler.mqtt_acknowledge_subscription(subscriptions['packet_id'], return_codes)
                for index, subscription in enumerate(subscriptions['topics']):
                    if return_codes[index] != 0x80:
                        yield from self.plugins_manager.fire_event(
                            EVENT_BROKER_CLIENT_SUBSCRIBED,
                            client_id=client_session.client_id,
                            topic=subscription[0],
                            qos=subscription[1])
                        yield from self.publish_retained_messages_for_subscription(subscription, client_session)
                subscribe_waiter = asyncio.Task(handler.get_next_pending_subscription(), loop=self._loop)
                self.logger.debug(repr(self._subscriptions))
            if wait_deliver in done:
                self.logger.debug("%s handling message delivery" % client_session.client_id)
                publish_packet = wait_deliver.result()
                packet_id = publish_packet.variable_header.packet_id
                topic_name = publish_packet.variable_header.topic_name
                data = publish_packet.payload.data
                yield from self.broadcast_application_message(client_session, topic_name, data)
                if publish_packet.retain_flag:
                    self.retain_message(client_session, topic_name, data)
                # Acknowledge message delivery
                yield from handler.mqtt_acknowledge_delivery(packet_id)
                wait_deliver = asyncio.Task(handler.mqtt_deliver_next_message(), loop=self._loop)
        disconnect_waiter.cancel()
        subscribe_waiter.cancel()
        unsubscribe_waiter.cancel()
        wait_deliver.cancel()

        self.logger.debug("%s Client disconnecting" % client_session.client_id)
        yield from self._stop_handler(handler)
        client_session.transitions.disconnect()
        yield from self.plugins_manager.fire_event(EVENT_BROKER_CLIENT_DISCONNECTED, client_id=client_session.client_id)
        yield from writer.close()
        self.logger.debug("%s Session disconnected" % client_session.client_id)
        server.release_connection()

    def _init_handler(self, session, reader, writer):
        """
        Create a BrokerProtocolHandler and attach to a session
        :return:
        """
        handler = BrokerProtocolHandler(self.plugins_manager, self._loop)
        handler.attach(session, reader, writer)
        handler.on_packet_received.connect(self.sys_handle_packet_received)
        handler.on_packet_sent.connect(self.sys_handle_packet_sent)
        return handler

    @asyncio.coroutine
    def _stop_handler(self, handler):
        """
        Stop a running handler and detach if from the session
        :param handler:
        :return:
        """
        try:
            yield from handler.stop()
        except Exception as e:
            self.logger.error(e)

    @asyncio.coroutine
    def authenticate(self, session: Session, listener):
        """
        This method call the authenticate method on registered plugins to test user authentication.
        User is considered authenticated if all plugins called returns True.
        Plugins authenticate() method are supposed to return :
         - True if user is authentication succeed
         - False if user authentication fails
         - None if authentication can't be achieved (then plugin result is then ignored)
        :param session:
        :param listener:
        :return:
        """
        auth_plugins = None
        auth_config = self.config.get('auth', None)
        if auth_config:
            auth_plugins = auth_config.get('plugins', None)
        returns = yield from self.plugins_manager.map_plugin_coro(
            "authenticate",
            session=session,
            filter_plugins=auth_plugins)
        auth_result = True
        if returns:
            for plugin in returns:
                res = returns[plugin]
                if res is False:
                    auth_result = False
                    self.logger.debug("Authentication failed due to '%s' plugin result: %s" % (plugin.name, res))
                else:
                    self.logger.debug("'%s' plugin result: %s" % (plugin.name, res))
        # If all plugins returned True, authentication is success
        return auth_result

    def retain_message(self, source_session, topic_name, data, qos=None):
        if data is not None and data != b'':
            # If retained flag set, store the message for further subscriptions
            self.logger.debug("Retaining message on topic %s" % topic_name)
            retained_message = RetainedApplicationMessage(source_session, topic_name, data, qos)
            self._global_retained_messages[topic_name] = retained_message
        else:
            # [MQTT-3.3.1-10]
            self.logger.debug("Clear retained messages for topic '%s'" % topic_name)
            del self._global_retained_messages[topic_name]

    def add_subscription(self, subscription, session):
        import re
        wildcard_pattern = re.compile('.*?/?\+/?.*?')
        try:
            a_filter = subscription[0]
            if '#' in a_filter and not a_filter.endswith('#'):
                # [MQTT-4.7.1-2] Wildcard character '#' is only allowed as last character in filter
                return 0x80
            if '+' in a_filter and not wildcard_pattern.match(a_filter):
                # [MQTT-4.7.1-3] + wildcard character must occupy entire level
                return 0x80

            qos = subscription[1]
            if 'max-qos' in self.config and qos > self.config['max-qos']:
                qos = self.config['max-qos']
            if a_filter not in self._subscriptions:
                self._subscriptions[a_filter] = []
            already_subscribed = next(
                (s for (s,qos) in self._subscriptions[a_filter] if s.client_id == session.client_id), None)
            if not already_subscribed:
                self._subscriptions[a_filter].append((session, qos))
            else:
                self.logger.debug("Client %s has already subscribed to %s" % (format_client_message(session=session), a_filter))
            return qos
        except KeyError:
            return 0x80

    def del_subscription(self, a_filter, session):
        try:
            subscriptions = self._subscriptions[a_filter]
            for index, (sub_session, qos) in enumerate(subscriptions):
                if sub_session.client_id == session.client_id:
                    self.logger.debug("Removing subscription on topic '%s' for client %s" %
                                      (a_filter, format_client_message(session=session)))
                    subscriptions.pop(index)
            # Remove filter for subsriptions list if there are no more subscribers
            if not self._subscriptions[a_filter]:
                del self._subscriptions[a_filter]
        except KeyError:
            # Unsubscribe topic not found in current subscribed topics
            pass

    def matches(self, topic, a_filter):
        import re
        match_pattern = re.compile(a_filter.replace('#', '.*').replace('$', '\$').replace('+', '[\$\s\w\d]+'))
        if match_pattern.match(topic):
            return True
        else:
            return False

    @asyncio.coroutine
    def broadcast_application_message(self, source_session, topic, data, force_qos=None):
        #self.logger.debug("Broadcasting message from %s on topic %s" %
        #                  (format_client_message(session=source_session), topic)
        #                  )
        publish_tasks = []
        try:
            for k_filter in self._subscriptions:
                if self.matches(topic, k_filter):
                    subscriptions = self._subscriptions[k_filter]
                    for (target_session, qos) in subscriptions:
                        if force_qos is not None:
                            qos = force_qos
                        if target_session.transitions.state == 'connected':
                            self.logger.debug("broadcasting application message from %s on topic '%s' to %s" %
                                              (format_client_message(session=source_session),
                                               topic, format_client_message(session=target_session)))
                            handler = _get_handler(target_session)
                            publish_tasks.append(
                                asyncio.Task(handler.mqtt_publish(topic, data, qos, retain=False), loop=self._loop)
                            )
                        else:
                            self.logger.debug("retaining application message from %s on topic '%s' to client '%s'" %
                                              (format_client_message(session=source_session),
                                               topic, format_client_message(session=target_session)))
                            retained_message = RetainedApplicationMessage(source_session, topic, data, qos)
                            publish_tasks.append(
                                asyncio.Task(target_session.retained_messages.put(retained_message), loop=self._loop)
                            )

            if publish_tasks:
                yield from asyncio.wait(publish_tasks, loop=self._loop)
        except Exception as e:
            self.logger.warn("Message broadcasting failed: %s", e)
        #self.logger.debug("End Broadcasting message from %s on topic %s" %
        #                  (format_client_message(session=source_session), topic)
        #                  )

    @asyncio.coroutine
    def publish_session_retained_messages(self, session):
        self.logger.debug("Publishing %d messages retained for session %s" %
                          (session.retained_messages.qsize(), format_client_message(session=session))
                          )
        publish_tasks = []
        while not session.retained_messages.empty():
            retained = yield from session.retained_messages.get()
            publish_tasks.append(asyncio.Task(
                session.handler.mqtt_publish(
                    retained.topic, retained.data, retained.qos, True), loop=self._loop))
        if publish_tasks:
            yield from asyncio.wait(publish_tasks, loop=self._loop)

    @asyncio.coroutine
    def publish_retained_messages_for_subscription(self, subscription, session):
        self.logger.debug("Begin broadcasting messages retained due to subscription on '%s' from %s" %
                          (subscription[0], format_client_message(session=session)))
        publish_tasks = []
        for d_topic in self._global_retained_messages:
            self.logger.debug("matching : %s %s" % (d_topic, subscription[0]))
            if self.matches(d_topic, subscription[0]):
                self.logger.debug("%s and %s match" % (d_topic, subscription[0]))
                retained = self._global_retained_messages[d_topic]
                publish_tasks.append(asyncio.Task(
                    session.handler.mqtt_publish(
                        retained.topic, retained.data, subscription[1], True), loop=self._loop))
        if publish_tasks:
            yield from asyncio.wait(publish_tasks, loop=self._loop)
        self.logger.debug("End broadcasting messages retained due to subscription on '%s' from %s" %
                          (subscription[0], format_client_message(session=session)))

    def delete_session(self, client_id):
        """
        Delete an existing session data, for example due to clean session set in CONNECT
        :param client_id:
        :return:
        """
        try:
            session = self._sessions[client_id][0]
        except KeyError:
            session = None
        if session is None:
            self.logger.warn("Delete session : session %s doesn't exist" % client_id)
            return

        # Delete subscriptions
        self.logger.debug("deleting session %s subscriptions" % repr(session))
        nb_sub = 0
        for a_filter in self._subscriptions:
            self.del_subscription(a_filter, session)
            nb_sub += 1
        self.logger.debug("%d subscriptions deleted" % nb_sub)

        self.logger.debug("deleting existing session %s" % repr(self._sessions[client_id]))
        del self._sessions[client_id]

    def sys_handle_packet_received(self, packet):
        if packet:
            packet_size = packet.bytes_length
            self._stats[STAT_BYTES_RECEIVED] += packet_size
            self._stats[STAT_MSG_RECEIVED] += 1
            if packet.fixed_header.packet_type == PUBLISH:
                self._stats[STAT_PUBLISH_RECEIVED] += 1

    def sys_handle_packet_sent(self, packet):
        if packet:
            packet_size = packet.bytes_length
            self._stats[STAT_BYTES_SENT] += packet_size
            self._stats[STAT_MSG_SENT] += 1
            if packet.fixed_header.packet_type == PUBLISH:
                self._stats[STAT_PUBLISH_SENT] += 1

    def _get_handler(self, session):
        """
        Return the handler attached to a given session
        :param session:
        :return:
        """
        (s, h) = self._sessions[session.client_id]
        return h
