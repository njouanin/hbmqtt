# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
from transitions import Machine
from asyncio import Queue
from collections import OrderedDict
from hbmqtt.mqtt.publish import PublishPacket
from hbmqtt.errors import HBMQTTException

OUTGOING = 0
INCOMING = 1


class ApplicationMessage:

    """
        ApplicationMessage and subclasses are used to store published message information flow. These objects can contain different information depending on the way they were created (incoming or outgoing) and the quality of service used between peers.
    """

    __slots__ = (
        'packet_id', 'topic', 'qos', 'data', 'retain', 'publish_packet',
        'puback_packet', 'pubrec_packet', 'pubrel_packet', 'pubcomp_packet',
    )

    def __init__(self, packet_id, topic, qos, data, retain):
        self.packet_id = packet_id
        """ Publish message `packet identifier <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718025>`_"""

        self.topic = topic
        """ Publish message topic"""

        self.qos = qos
        """ Publish message Quality of Service"""

        self.data = data
        """ Publish message payload data"""

        self.retain = retain
        """ Publish message retain flag"""

        self.publish_packet = None
        """ :class:`hbmqtt.mqtt.publish.PublishPacket` instance corresponding to the `PUBLISH <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718037>`_ packet in the messages flow. ``None`` if the PUBLISH packet has not already been received or sent."""

        self.puback_packet = None
        """ :class:`hbmqtt.mqtt.puback.PubackPacket` instance corresponding to the `PUBACK <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718043>`_ packet in the messages flow. ``None`` if QoS != QOS_1 or if the PUBACK packet has not already been received or sent."""

        self.pubrec_packet = None
        """ :class:`hbmqtt.mqtt.puback.PubrecPacket` instance corresponding to the `PUBREC <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718048>`_ packet in the messages flow. ``None`` if QoS != QOS_2 or if the PUBREC packet has not already been received or sent."""

        self.pubrel_packet = None
        """ :class:`hbmqtt.mqtt.puback.PubrelPacket` instance corresponding to the `PUBREL <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718053>`_ packet in the messages flow. ``None`` if QoS != QOS_2 or if the PUBREL packet has not already been received or sent."""

        self.pubcomp_packet = None
        """ :class:`hbmqtt.mqtt.puback.PubrelPacket` instance corresponding to the `PUBCOMP <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718058>`_ packet in the messages flow. ``None`` if QoS != QOS_2 or if the PUBCOMP packet has not already been received or sent."""

    def build_publish_packet(self, dup=False):
        """
            Build :class:`hbmqtt.mqtt.publish.PublishPacket` from attributes

        :param dup: force dup flag
        :return: :class:`hbmqtt.mqtt.publish.PublishPacket` built from ApplicationMessage instance attributes
        """
        return PublishPacket.build(self.topic, self.data, self.packet_id, dup, self.qos, self.retain)

    def __eq__(self, other):
        return self.packet_id == other.packet_id


class IncomingApplicationMessage(ApplicationMessage):

    """
        Incoming :class:`~hbmqtt.session.ApplicationMessage`.
    """

    __slots__ = ('direction',)

    def __init__(self, packet_id, topic, qos, data, retain):
        super().__init__(packet_id, topic, qos, data, retain)
        self.direction = INCOMING


class OutgoingApplicationMessage(ApplicationMessage):

    """
        Outgoing :class:`~hbmqtt.session.ApplicationMessage`.
    """

    __slots__ = ('direction',)

    def __init__(self, packet_id, topic, qos, data, retain):
        super().__init__(packet_id, topic, qos, data, retain)
        self.direction = OUTGOING


class Session:
    states = ['new', 'connected', 'disconnected']

    def __init__(self, loop=None):
        self._init_states()
        self.remote_address = None
        self.remote_port = None
        self.client_id = None
        self.clean_session = None
        self.will_flag = False
        self.will_message = None
        self.will_qos = None
        self.will_retain = None
        self.will_topic = None
        self.keep_alive = 0
        self.publish_retry_delay = 0
        self.broker_uri = None
        self.username = None
        self.password = None
        self.cafile = None
        self.capath = None
        self.cadata = None
        self._packet_id = 0
        self.parent = 0
        if loop is not None:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()

        # Used to store outgoing ApplicationMessage while publish protocol flows
        self.inflight_out = OrderedDict()

        # Used to store incoming ApplicationMessage while publish protocol flows
        self.inflight_in = OrderedDict()

        # Stores messages retained for this session
        self.retained_messages = Queue(loop=self._loop)

        # Stores PUBLISH messages ID received in order and ready for application process
        self.delivered_message_queue = Queue(loop=self._loop)

    def _init_states(self):
        self.transitions = Machine(states=Session.states, initial='new')
        self.transitions.add_transition(trigger='connect', source='new', dest='connected')
        self.transitions.add_transition(trigger='connect', source='disconnected', dest='connected')
        self.transitions.add_transition(trigger='disconnect', source='connected', dest='disconnected')
        self.transitions.add_transition(trigger='disconnect', source='new', dest='disconnected')
        self.transitions.add_transition(trigger='disconnect', source='disconnected', dest='disconnected')

    @property
    def next_packet_id(self):
        self._packet_id += 1
        if self._packet_id > 65535:
            self._packet_id = 1
        while self._packet_id in self.inflight_in or self._packet_id in self.inflight_out:
            self._packet_id += 1
            if self._packet_id > 65535:
                raise HBMQTTException("More than 65525 messages pending. No free packet ID")

        return self._packet_id

    @property
    def inflight_in_count(self):
        return len(self.inflight_in)

    @property
    def inflight_out_count(self):
        return len(self.inflight_out)

    @property
    def retained_messages_count(self):
        return self.retained_messages.qsize()

    def __repr__(self):
        return type(self).__name__ + '(clientId={0}, state={1})'.format(self.client_id, self.transitions.state)

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        # del state['transitions']
        del state['retained_messages']
        del state['delivered_message_queue']
        return state

    def __setstate(self, state):
        self.__dict__.update(state)
        self.retained_messages = Queue()
        self.delivered_message_queue = Queue()

    def __eq__(self, other):
        return self.client_id == other.client_id
