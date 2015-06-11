# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from enum import Enum

class SessionState(Enum):
    NEW = 0
    CONNECTED = 1
    DISCONNECTED = 2

class Session:
    def __init__(self):
        self.state = SessionState.NEW
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
        self.keep_alive = None
        self.username = None
        self.password = None
        self.scheme = None
        self._last_exception = None
