# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import enum

class ClientState(enum):
    CONNECTED = 1
    DISCONNECTED = 2

class Session:
    def __init__(self, remote_address, remote_port, client_id, clean_session):
        self.remote_address = remote_address
        self.remote_port = remote_port
        self.client_id = client_id
        self.clean_session = clean_session
        self.client_state = ClientState.CONNECTED
        self.will_flag = False
        self.will_message = None
        self.will_qos = None
        self.will_retain = None
        self.keep_alive = 0
