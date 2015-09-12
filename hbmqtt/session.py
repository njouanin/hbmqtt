# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from transitions import Machine, MachineError
from asyncio import Queue
from collections import OrderedDict
from hbmqtt.mqtt.constants import *
from hbmqtt.mqtt.publish import PublishPacket
from hbmqtt.mqtt.puback import PubackPacket


class ApplicationMessage:
    states = ['new', 'published', 'acknowledged', 'received', 'released', 'completed']

    def __init__(self, packet_id, topic, qos, data, retain):
        self.packet_id = packet_id
        self.topic = topic
        self.qos = qos
        self.data = data
        self.retain = retain
        self.publish_packet = None
        self.puback_packet = None
        self.pubrec_packet = None
        self.pubrel_packet = None
        self.pubcomp_packet = None

    def build_publish_packet(self, dup=False):
        return PublishPacket.build(self.topic, self.data, self.packet_id, dup, self.qos, self.retain)


class IncomingApplicationMessage(ApplicationMessage):
    pass


class OutgoingApplicationMessage(ApplicationMessage):
    pass


class Session:
    states = ['new', 'connected', 'disconnected']

    def __init__(self):
        self._init_states()
        self.reader = None
        self.writer = None
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

        # Used to store outgoing ApplicationMessage while publish protocol flows
        self.inflight_out = OrderedDict()

        # Used to store incoming ApplicationMessage while publish protocol flows
        self.inflight_in = OrderedDict()

        # Stores messages retained for this session
        self.retained_messages = Queue()

        # Stores PUBLISH messages ID received in order and ready for application process
        self.delivered_message_queue = Queue()

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
