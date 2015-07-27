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
    'bind-port': 1883,
    'timeout-disconnect-delay': 10
}


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
        self._init_states()
        self._sessions = dict()
        self._subscriptions = dict()
        self._global_retained_messages = dict()

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
        extra_info = writer.get_extra_info('peername')
        remote_address = extra_info[0]
        remote_port = extra_info[1]
        self.logger.debug("Connection from %s:%d" % (remote_address, remote_port))

        # Wait for first packet and expect a CONNECT
        connect = None
        try:
            connect = yield from ConnectPacket.from_stream(reader)
            self.logger.debug(" <-in-- " + repr(connect))
            self.check_connect(connect)
        except HBMQTTException as exc:
            self.logger.warn("[MQTT-3.1.0-1] %s: Can't read first packet an CONNECT: %s" %
                             (format_client_message(address=remote_address, port=remote_port), exc))
            writer.close()
            self.logger.debug("Connection closed")
            return
        except BrokerException as be:
            self.logger.error('Invalid connection from %s : %s' %
                              (format_client_message(address=remote_address, port=remote_port), be))
            writer.close()
            self.logger.debug("Connection closed")
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

        client_session = None
        self.logger.debug("Clean session={0}".format(connect.variable_header.clean_session_flag))
        self.logger.debug("known sessions={0}".format(self._sessions))
        if connect.variable_header.clean_session_flag:
            client_id = connect.payload.client_id
            if client_id is not None:
                self.delete_session(client_id)
            client_session = Session()
            client_session.parent = 0
            self._sessions[client_id] = client_session
        else:
            # Get session from cache
            client_id = connect.payload.client_id
            if client_id in self._sessions:
                self.logger.debug("Found old session %s" % repr(self._sessions[client_id]))
                client_session = self._sessions[client_id]
                client_session.parent = 1
            else:
                client_session = Session()
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

        client_session.reader = reader
        client_session.writer = writer

        if self.authenticate(client_session):
            connack = ConnackPacket.build(client_session.parent, ReturnCode.CONNECTION_ACCEPTED)
            self.logger.info('%s : connection accepted' % format_client_message(session=client_session))
            self.logger.debug(" -out-> " + repr(connack))
            yield from connack.to_stream(writer)
        else:
            connack = ConnackPacket.build(client_session.parent, ReturnCode.NOT_AUTHORIZED)
            self.logger.info('%s : connection refused' % format_client_message(session=client_session))
            self.logger.debug(" -out-> " + repr(connack))
            yield from connack.to_stream(writer)
            writer.close()
            return

        client_session.machine.connect()
        handler = BrokerProtocolHandler(self._loop)
        handler.attach_to_session(client_session)
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
                    #Connection closed anormally, send will message
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
        try:
            yield from handler.stop()
        except Exception as e:
            self.logger.error(e)
        finally:
            handler.detach_from_session()
            handler = None
        client_session.machine.disconnect()
        writer.close()
        self.logger.debug("%s Session disconnected" % client_session.client_id)

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
            self.logger.debug("%s Retaining message on topic %s" % (source_session.client_id, topic_name))
            retained_message = RetainedApplicationMessage(source_session, topic_name, data, qos)
            self._global_retained_messages[topic_name] = retained_message
        else:
            # [MQTT-3.3.1-10]
            self.logger.debug("%s Clear retained messages for topic '%s'" % (source_session.client_id, topic_name))
            del self._global_retained_messages[topic_name]

    def add_subscription(self, subscription, session):
        import re
        #wildcard_pattern = re.compile('(/.+?\+)|(/\+.+?)|(/.+?\+.+?)')
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

    def matches(self, topic, filter):
        import re
        match_pattern = re.compile(filter.replace('#', '.*').replace('+', '[\s\w\d]+'))
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
        for filter in self._subscriptions:
            self.del_subscription(filter, session)
            nb_sub += 1
        self.logger.debug("%d subscriptions deleted" % nb_sub)

        self.logger.debug("deleting existing session %s" % repr(self._sessions[client_id]))
        del self._sessions[client_id]
