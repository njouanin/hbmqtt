# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from enum import Enum
from transitions import Machine, MachineError
from asyncio import Queue

class Session:
    states = ['new', 'connected', 'disconnected']

    def __init__(self):
        self._init_states()
        self.reader = None
        self.writer = None
        self.remote_address = None
        self.remote_port = None
        self.local_address = None
        self.local_port = None
        self.client_id = None
        self.clean_session = None
        self.will_flag = False
        self.will_message = None
        self.will_qos = None
        self.will_retain = None
        self.will_topic = None
        self.keep_alive = 0
        self.username = None
        self.password = None
        self.scheme = None
        self._packet_id = 0
        self.parent = 0
        self.handler = None

        # Used to store outgoing InflightMessage while publish protocol flows
        self.outgoing_msg = dict()

        # Used to store incoming InflightMessage while publish protocol flows
        self.incoming_msg = dict()

        # Stores messages retained for this session
        self.retained_messages = Queue()

        # Stores PUBLISH messages ID received in order and ready for application process
        self.delivered_message_queue = Queue()

    def _init_states(self):
        self.machine = Machine(states=Session.states, initial='new')
        self.machine.add_transition(trigger='connect', source='new', dest='connected')
        self.machine.add_transition(trigger='connect', source='disconnected', dest='connected')
        self.machine.add_transition(trigger='disconnect', source='connected', dest='disconnected')

    @property
    def next_packet_id(self):
        self._packet_id += 1
        return self._packet_id

    def __repr__(self):
        return type(self).__name__ + '(clientId={0}, state={1})'.format(self.client_id, self.machine.state)
