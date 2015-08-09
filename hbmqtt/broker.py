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
from hbmqtt.mqtt.connect import ConnectPacket
from hbmqtt.mqtt.connack import *
from hbmqtt.errors import HBMQTTException
from hbmqtt.utils import format_client_message, gen_client_id
from hbmqtt.codecs import int_to_bytes_str
from hbmqtt.adapters import (
    StreamReaderAdapter,
    StreamWriterAdapter,
    ReaderAdapter,
    WriterAdapter,
    WebSocketsReader,
    WebSocketsWriter)


_defaults = {
    'timeout-disconnect-delay': 2,
    'publish-retry-delay': 5,
}

DOLLAR_SYS_ROOT = '$SYS/broker/'
STAT_BYTES_SENT = 'bytes_sent'
STAT_BYTES_RECEIVED = 'bytes_received'
STAT_MSG_SENT = 'messages_sent'
STAT_MSG_RECEIVED = 'messages_received'
STAT_UPTIME = 'uptime'

class BrokerException(BaseException):
    pass


class Subscription:
    def __init__(self, session, qos):
        self.session = session
        self.qos = qos

    def __repr__(self):
        return type(self).__name__ + '(client_id={0}, qos={1!r})'.format(self.session.client_id, self.qos)


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
            yield self.instance.wait_closed()


class Broker:
    states = ['new', 'starting', 'started', 'not_started', 'stopping', 'stopped', 'not_stopped', 'stopped']

    def __init__(self, config=None, loop=None):
        """

        :param config: Example Yaml config
            listeners:
                - default:  #Mandatory
                    max-connections: 50000
                    type: tcp
                - my-tcp-1:
                    bind: 127.0.0.1:1883
                - my-tcp-2:
                    bind: 1.2.3.4:1883
                    max-connections: 1000
                - my-tcp-ssl-1:
                    bind: 127.0.0.1:8883
                    ssl: on
                    cafile: /some/cafile
                    capath: /some/folder
                    capath: certificate data
                    certfile: /some/certfile
                    keyfile: /some/key
                - my-ws-1:
                    bind: 0.0.0.0:8080
                    type: ws
            timeout-disconnect-delay: 2
            publish-retry-delay: 5

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

    def _build_listeners_config(self, broker_config):
        self.listeners_config = dict()
        try:
            listeners_config = broker_config['listeners']
            defaults = listeners_config['default']
            for listener in listeners_config:
                if listener != 'default':
                    config = dict(defaults)
                    config.update(listeners_config[listener])
                    self.listeners_config[listener] = config
        except KeyError as ke:
            raise BrokerException("Listener config not found invalid: %s" % ke)

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

        # Clear broker stats
        self._clear_stats()
        try:
            # Start network listeners
            for listener_name in self.listeners_config:
                listener = self.listeners_config[listener_name]
                self.logger.info("Binding listener '%s' to %s" % (listener_name, listener['bind']))

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

            # Start $SYS topics management
            self._init_dollar_sys()
            try:
                sys_interval = int(self.config['sys_interval'])
                if sys_interval:
                    self.logger.debug("Setup $SYS broadcasting every %d secondes" % sys_interval)
                    self.sys_handle = self._loop.call_later(sys_interval, self.broadcast_dollar_sys_topics)
            except KeyError:
                pass
                # 'sys_internal' config parameter not found

            self.machine.starting_success()
            self.logger.debug("Broker started")
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

        # Stop $SYS topics broadcasting
        if self.sys_handle:
            self.sys_handle.cancel()

        for listener_name in self._servers:
            server = self._servers[listener_name]
            yield from server.close_instance()
        self.logger.debug("Broker closing")
        self.logger.info("Broker closed")
        self.machine.stopping_success()

    def _clear_stats(self):
        """
        Initializes broker statistics data structures
        """
        for stat in (STAT_BYTES_RECEIVED,
                     STAT_BYTES_SENT,
                     STAT_MSG_RECEIVED,
                     STAT_MSG_SENT,
                     'clients_connected'):
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
        client_connected = sum(1 for k, session in self._sessions.items() if session.machine.state == 'connected')
        client_disconnected = sum(1 for k, session in self._sessions.items() if session.machine.state == 'disconnected')
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
        ]

        # Wait until broadcasting tasks end
        if len(tasks) > 0:
            asyncio.wait(tasks)
        # Reschedule
        sys_interval = int(self.config['sys_interval'])
        self.logger.debug("Broadcasting $SYS topics")
        self.sys_handle = self._loop.call_later(sys_interval, self.broadcast_dollar_sys_topics)

    def _broadcast_sys_topic(self, topic_basename, data):
        return self._internal_message_broadcast(DOLLAR_SYS_ROOT + topic_basename, data)

    def _internal_message_broadcast(self, topic, data):
        return asyncio.Task(self.broadcast_application_message(None, topic, data))

    @asyncio.coroutine
    def ws_connected(self, websocket, uri, listener_name):
        yield from self.client_connected(listener_name, WebSocketsReader(websocket), WebSocketsWriter(websocket))

    @asyncio.coroutine
    def stream_connected(self, reader, writer, listener_name):
        yield from self.client_connected(listener_name, StreamReaderAdapter(reader), StreamWriterAdapter(writer))

    @asyncio.coroutine
    def client_connected(self, listener_name, reader: ReaderAdapter, writer: WriterAdapter):
        # Wait for connection available
        server = self._servers[listener_name]
        yield from server.acquire_connection()

        remote_address, remote_port = writer.get_peer_info()
        self.logger.debug("Connection from %s:%d on listener '%s'" % (remote_address, remote_port, listener_name))

        # Wait for first packet and expect a CONNECT
        connect = None
        try:
            connect = yield from ConnectPacket.from_stream(reader)
            self.logger.debug(" <-in-- " + repr(connect))
            self.check_connect(connect)
        except HBMQTTException as exc:
            self.logger.warn("[MQTT-3.1.0-1] %s: Can't read first packet an CONNECT: %s" %
                             (format_client_message(address=remote_address, port=remote_port), exc))
            yield from writer.close()
            self.logger.debug("Connection closed")
            return
        except BrokerException as be:
            self.logger.error('Invalid connection from %s : %s' %
                              (format_client_message(address=remote_address, port=remote_port), be))
            yield from writer.close()
            self.logger.debug("Connection closed")
            return

        connack = None
        if connect.variable_header.proto_level != 4:
            # only MQTT 3.1.1 supported
            self.logger.error('Invalid protocol from %s: %d' %
                              (format_client_message(address=remote_address, port=remote_port),
                               connect.variable_header.protocol_level))
            connack = ConnackPacket.build(0, UNACCEPTABLE_PROTOCOL_VERSION)  # [MQTT-3.2.2-4] session_parent=0
        elif connect.variable_header.username_flag and connect.payload.username is None:
            self.logger.error('Invalid username from %s' %
                              (format_client_message(address=remote_address, port=remote_port)))
            connack = ConnackPacket.build(0, BAD_USERNAME_PASSWORD)  # [MQTT-3.2.2-4] session_parent=0
        elif connect.variable_header.password_flag and connect.payload.password is None:
            self.logger.error('Invalid password %s' % (format_client_message(address=remote_address, port=remote_port)))
            connack = ConnackPacket.build(0, BAD_USERNAME_PASSWORD)  # [MQTT-3.2.2-4] session_parent=0
        elif connect.variable_header.clean_session_flag is False and connect.payload.client_id is None:
            self.logger.error('[MQTT-3.1.3-8] [MQTT-3.1.3-9] %s: No client Id provided (cleansession=0)' %
                              format_client_message(address=remote_address, port=remote_port))
            connack = ConnackPacket.build(0, IDENTIFIER_REJECTED)
            self.logger.debug(" -out-> " + repr(connack))
        if connack is not None:
            self.logger.debug(" -out-> " + repr(connack))
            yield from connack.to_stream(writer)
            yield from writer.close()
            return

        client_session = None
        self.logger.debug("Clean session={0}".format(connect.variable_header.clean_session_flag))
        self.logger.debug("known sessions={0}".format(self._sessions))
        client_id = connect.payload.client_id
        if connect.variable_header.clean_session_flag:
            # Delete existing session and create a new one
            if client_id is not None:
                self.delete_session(client_id)
            client_session = Session()
            client_session.parent = 0
            client_session.client_id = client_id
            self._sessions[client_id] = client_session
        else:
            # Get session from cache
            if client_id in self._sessions:
                self.logger.debug("Found old session %s" % repr(self._sessions[client_id]))
                client_session = self._sessions[client_id]
                client_session.parent = 1
            else:
                client_session = Session()
                client_session.client_id = client_id
                self._sessions[client_id] = client_session
                client_session.parent = 0

        if client_session.client_id is None:
            # Generate client ID
            client_session.client_id = gen_client_id()
        client_session.remote_address = remote_address
        client_session.remote_port = remote_port
        client_session.clean_session = connect.variable_header.clean_session_flag
        client_session.will_flag = connect.variable_header.will_flag
        client_session.will_retain = connect.variable_header.will_retain_flag
        client_session.will_qos = connect.variable_header.will_qos
        client_session.will_topic = connect.payload.will_topic
        client_session.will_message = connect.payload.will_message
        client_session.username = connect.payload.username
        client_session.password = connect.payload.password
        client_session.client_id = connect.payload.client_id
        if connect.variable_header.keep_alive > 0:
            client_session.keep_alive = connect.variable_header.keep_alive + self.config['timeout-disconnect-delay']
        else:
            client_session.keep_alive = 0
        client_session.publish_retry_delay = self.config['publish-retry-delay']

        client_session.reader = reader
        client_session.writer = writer

        if self.authenticate(client_session):
            connack = ConnackPacket.build(client_session.parent, CONNECTION_ACCEPTED)
            self.logger.info('%s : connection accepted' % format_client_message(session=client_session))
            self.logger.debug(" -out-> " + repr(connack))
            yield from connack.to_stream(writer)
        else:
            connack = ConnackPacket.build(client_session.parent, NOT_AUTHORIZED)
            self.logger.info('%s : connection refused' % format_client_message(session=client_session))
            self.logger.debug(" -out-> " + repr(connack))
            yield from connack.to_stream(writer)
            yield from writer.close()
            return

        client_session.machine.connect()
        handler = self._init_handler(reader, writer, client_session)
        self.logger.debug("%s Start messages handling" % client_session.client_id)
        yield from handler.start()
        self.logger.debug("Retained messages queue size: %d" % client_session.retained_messages.qsize())
        yield from self.publish_session_retained_messages(client_session)
        self.logger.debug("%s Wait for disconnect" % client_session.client_id)

        connected = True
        wait_disconnect = asyncio.Task(handler.wait_disconnect())
        wait_subscription = asyncio.Task(handler.get_next_pending_subscription())
        wait_unsubscription = asyncio.Task(handler.get_next_pending_unsubscription())
        wait_deliver = asyncio.Task(handler.mqtt_deliver_next_message())
        while connected:
            done, pending = yield from asyncio.wait(
                [wait_disconnect, wait_subscription, wait_unsubscription, wait_deliver],
                return_when=asyncio.FIRST_COMPLETED)
            if wait_disconnect in done:
                result = wait_disconnect.result()
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
            if wait_unsubscription in done:
                self.logger.debug("%s handling unsubscription" % client_session.client_id)
                unsubscription = wait_unsubscription.result()
                for topic in unsubscription['topics']:
                    self.del_subscription(topic, client_session)
                yield from handler.mqtt_acknowledge_unsubscription(unsubscription['packet_id'])
                wait_unsubscription = asyncio.Task(handler.get_next_pending_unsubscription())
            if wait_subscription in done:
                self.logger.debug("%s handling subscription" % client_session.client_id)
                subscriptions = wait_subscription.result()
                return_codes = []
                for subscription in subscriptions['topics']:
                    return_codes.append(self.add_subscription(subscription, client_session))
                yield from handler.mqtt_acknowledge_subscription(subscriptions['packet_id'], return_codes)
                for index, subscription in enumerate(subscriptions['topics']):
                    if return_codes[index] != 0x80:
                        yield from self.publish_retained_messages_for_subscription(subscription, client_session)
                wait_subscription = asyncio.Task(handler.get_next_pending_subscription())
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
                wait_deliver = asyncio.Task(handler.mqtt_deliver_next_message())
        wait_subscription.cancel()
        wait_unsubscription.cancel()
        wait_deliver.cancel()

        self.logger.debug("%s Client disconnecting" % client_session.client_id)
        yield from self._stop_handler(handler)
        client_session.machine.disconnect()
        yield from writer.close()
        self.logger.debug("%s Session disconnected" % client_session.client_id)
        server.release_connection()

    def _init_handler(self, reader, writer, session):
        """
        Create a BrokerProtocolHandler and attach to a session
        :return:
        """
        handler = BrokerProtocolHandler(reader, writer, self._loop)
        handler.attach_to_session(session)
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
        finally:
            handler.detach_from_session()

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
            a_filter = subscription['filter']
            if '#' in a_filter and not a_filter.endswith('#'):
                # [MQTT-4.7.1-2] Wildcard character '#' is only allowed as last character in filter
                return 0x80
            if '+' in a_filter and not wildcard_pattern.match(a_filter):
                # [MQTT-4.7.1-3] + wildcard character must occupy entire level
                return 0x80

            qos = subscription['qos']
            if 'max-qos' in self.config and qos > self.config['max-qos']:
                qos = self.config['max-qos']
            if a_filter not in self._subscriptions:
                self._subscriptions[a_filter] = []
            already_subscribed = next(
                (s for s in self._subscriptions[a_filter] if s.session.client_id == session.client_id), None)
            if not already_subscribed:
                self._subscriptions[a_filter].append(Subscription(session, qos))
            else:
                self.logger.debug("Client %s has already subscribed to %s" % (format_client_message(session=session), a_filter))
            return qos
        except KeyError:
            return 0x80

    def del_subscription(self, a_filter, session):
        try:
            subscriptions = self._subscriptions[a_filter]
            for index, subscription in enumerate(subscriptions):
                if subscription.session.client_id == session.client_id:
                    self.logger.debug("Removing subscription on topic '%s' for client %s" %
                                      (a_filter, format_client_message(session=session)))
                    subscriptions.pop(index)
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
        self.logger.debug("Broadcasting message from %s on topic %s" %
                          (format_client_message(session=source_session), topic)
                          )
        self.logger.debug("Current subscriptions: %s" % repr(self._subscriptions))
        publish_tasks = []
        try:
            for k_filter in self._subscriptions:
                if self.matches(topic, k_filter):
                    subscriptions = self._subscriptions[k_filter]
                    for subscription in subscriptions:
                        target_session = subscription.session
                        qos = subscription.qos
                        if force_qos is not None:
                            qos = force_qos
                        if target_session.machine.state == 'connected':
                            self.logger.debug("broadcasting application message from %s on topic '%s' to %s" %
                                              (format_client_message(session=source_session),
                                               topic, format_client_message(session=target_session)))
                            handler = subscription.session.handler
                            publish_tasks.append(
                                asyncio.Task(handler.mqtt_publish(topic, data, qos, retain=False))
                            )
                        else:
                            self.logger.debug("retaining application message from %s on topic '%s' to client '%s'" %
                                              (format_client_message(session=source_session),
                                               topic, format_client_message(session=target_session)))
                            retained_message = RetainedApplicationMessage(source_session, topic, data, qos)
                            publish_tasks.append(
                                asyncio.Task(target_session.retained_messages.put(retained_message))
                            )

            if len(publish_tasks) > 0:
                asyncio.wait(publish_tasks)
        except Exception as e:
            self.logger.warn("Message broadcasting failed: %s", e)
        self.logger.debug("End Broadcasting message from %s on topic %s" %
                          (format_client_message(session=source_session), topic)
                          )
        for client_id in self._sessions:
            self.logger.debug("%s Retained messages queue size: %d" %
                              (client_id, self._sessions[client_id].retained_messages.qsize()))

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
                    retained.topic, retained.data, retained.qos, True)))
        if len(publish_tasks) > 0:
            asyncio.wait(publish_tasks)

    @asyncio.coroutine
    def publish_retained_messages_for_subscription(self, subscription, session):
        self.logger.debug("Begin broadcasting messages retained due to subscription on '%s' from %s" %
                          (subscription['filter'], format_client_message(session=session)))
        publish_tasks = []
        for d_topic in self._global_retained_messages:
            self.logger.debug("matching : %s %s" % (d_topic, subscription['filter']))
            if self.matches(d_topic, subscription['filter']):
                self.logger.debug("%s and %s match" % (d_topic, subscription['filter']))
                retained = self._global_retained_messages[d_topic]
                publish_tasks.append(asyncio.Task(
                    session.handler.mqtt_publish(
                        retained.topic, retained.data, subscription['qos'], True)))
        if len(publish_tasks) > 0:
            asyncio.wait(publish_tasks)
        self.logger.debug("End broadcasting messages retained due to subscription on '%s' from %s" %
                          (subscription['filter'], format_client_message(session=session)))

    def delete_session(self, client_id):
        """
        Delete an existing session data, for example due to clean session set in CONNECT
        :param client_id:
        :return:
        """
        try:
            session = self._sessions[client_id]
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

    def sys_handle_packet_sent(self, packet):
        if packet:
            packet_size = packet.bytes_length
            self._stats[STAT_BYTES_SENT] += packet_size
            self._stats[STAT_MSG_SENT] += 1
