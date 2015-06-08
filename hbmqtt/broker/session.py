# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

class Session:
    def __init__(self, remote_address, remote_port, client_id):
        self.remote_address = remote_address
        self.remote_port = remote_port
        self.client_id = client_id
