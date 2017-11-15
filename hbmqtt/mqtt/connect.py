# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio

from hbmqtt.codecs import bytes_to_int, decode_data_with_length, decode_string, encode_data_with_length, encode_string, int_to_bytes, read_or_raise
from hbmqtt.mqtt.packet import MQTTPacket, MQTTFixedHeader, CONNECT, MQTTVariableHeader, MQTTPayload
from hbmqtt.errors import HBMQTTException, NoDataException
from hbmqtt.adapters import ReaderAdapter
from hbmqtt.utils import gen_client_id


class ConnectVariableHeader(MQTTVariableHeader):

    __slots__ = ('proto_name', 'proto_level', 'flags', 'keep_alive')

    USERNAME_FLAG = 0x80
    PASSWORD_FLAG = 0x40
    WILL_RETAIN_FLAG = 0x20
    WILL_FLAG = 0x04
    WILL_QOS_MASK = 0x18
    CLEAN_SESSION_FLAG = 0x02
    RESERVED_FLAG = 0x01

    def __init__(self, connect_flags=0x00, keep_alive=0, proto_name='MQTT', proto_level=0x04):
        super().__init__()
        self.proto_name = proto_name
        self.proto_level = proto_level
        self.flags = connect_flags
        self.keep_alive = keep_alive

    def __repr__(self):
        return "ConnectVariableHeader(proto_name={0}, proto_level={1}, flags={2}, keepalive={3})".format(
            self.proto_name, self.proto_level, hex(self.flags), self.keep_alive)

    def _set_flag(self, val, mask):
        if val:
            self.flags |= mask
        else:
            self.flags &= ~mask

    def _get_flag(self, mask):
        if self.flags & mask:
            return True
        else:
            return False

    @property
    def username_flag(self) -> bool:
        return self._get_flag(self.USERNAME_FLAG)

    @username_flag.setter
    def username_flag(self, val: bool):
        self._set_flag(val, self.USERNAME_FLAG)

    @property
    def password_flag(self) -> bool:
        return self._get_flag(self.PASSWORD_FLAG)

    @password_flag.setter
    def password_flag(self, val: bool):
        self._set_flag(val, self.PASSWORD_FLAG)

    @property
    def will_retain_flag(self) -> bool:
        return self._get_flag(self.WILL_RETAIN_FLAG)

    @will_retain_flag.setter
    def will_retain_flag(self, val: bool):
        self._set_flag(val, self.WILL_RETAIN_FLAG)

    @property
    def will_flag(self) -> bool:
        return self._get_flag(self.WILL_FLAG)

    @will_flag.setter
    def will_flag(self, val: bool):
        self._set_flag(val, self.WILL_FLAG)

    @property
    def clean_session_flag(self) -> bool:
        return self._get_flag(self.CLEAN_SESSION_FLAG)

    @clean_session_flag.setter
    def clean_session_flag(self, val: bool):
        self._set_flag(val, self.CLEAN_SESSION_FLAG)

    @property
    def reserved_flag(self) -> bool:
        return self._get_flag(self.RESERVED_FLAG)

    @property
    def will_qos(self):
        return (self.flags & 0x18) >> 3

    @will_qos.setter
    def will_qos(self, val: int):
        self.flags &= 0xe7  # Reset QOS flags
        self.flags |= (val << 3)

    @classmethod
    @asyncio.coroutine
    def from_stream(cls, reader: ReaderAdapter, fixed_header: MQTTFixedHeader):
        #  protocol name
        protocol_name = yield from decode_string(reader)

        # protocol level
        protocol_level_byte = yield from read_or_raise(reader, 1)
        protocol_level = bytes_to_int(protocol_level_byte)

        # flags
        flags_byte = yield from read_or_raise(reader, 1)
        flags = bytes_to_int(flags_byte)

        # keep-alive
        keep_alive_byte = yield from read_or_raise(reader, 2)
        keep_alive = bytes_to_int(keep_alive_byte)

        return cls(flags, keep_alive, protocol_name, protocol_level)

    def to_bytes(self):
        out = bytearray()

        # Protocol name
        out.extend(encode_string(self.proto_name))
        # Protocol level
        out.append(self.proto_level)
        # flags
        out.append(self.flags)
        # keep alive
        out.extend(int_to_bytes(self.keep_alive, 2))

        return out


class ConnectPayload(MQTTPayload):

    __slots__ = (
        'client_id',
        'will_topic',
        'will_message',
        'username',
        'password',
        'client_id_is_random',
    )

    def __init__(self, client_id=None, will_topic=None, will_message=None, username=None, password=None):
        super().__init__()
        self.client_id_is_random = False
        self.client_id = client_id
        self.will_topic = will_topic
        self.will_message = will_message
        self.username = username
        self.password = password

    def __repr__(self):
        return "ConnectVariableHeader(client_id={0}, will_topic={1}, will_message={2}, username={3}, password={4})".\
            format(self.client_id, self.will_topic, self.will_message, self.username, self.password)

    @classmethod
    @asyncio.coroutine
    def from_stream(cls, reader: ReaderAdapter, fixed_header: MQTTFixedHeader,
                    variable_header: ConnectVariableHeader):
        payload = cls()
        #  Client identifier
        try:
            payload.client_id = yield from decode_string(reader)
        except NoDataException:
            payload.client_id = None

        if (payload.client_id is None or payload.client_id == ""):
            # A Server MAY allow a Client to supply a ClientId that has a length of zero bytes
            # [MQTT-3.1.3-6]
            payload.client_id = gen_client_id()
            # indicator to trow exception in case CLEAN_SESSION_FLAG is set to False
            payload.client_id_is_random = True

        # Read will topic, username and password
        if variable_header.will_flag:
            try:
                payload.will_topic = yield from decode_string(reader)
                payload.will_message = yield from decode_data_with_length(reader)
            except NoDataException:
                payload.will_topic = None
                payload.will_message = None

        if variable_header.username_flag:
            try:
                payload.username = yield from decode_string(reader)
            except NoDataException:
                payload.username = None

        if variable_header.password_flag:
            try:
                payload.password = yield from decode_string(reader)
            except NoDataException:
                payload.password = None

        return payload

    def to_bytes(self, fixed_header: MQTTFixedHeader, variable_header: ConnectVariableHeader):
        out = bytearray()
        # Client identifier
        out.extend(encode_string(self.client_id))
        # Will topic / message
        if variable_header.will_flag:
            out.extend(encode_string(self.will_topic))
            out.extend(encode_data_with_length(self.will_message))
        # username
        if variable_header.username_flag:
            out.extend(encode_string(self.username))
        # password
        if variable_header.password_flag:
            out.extend(encode_string(self.password))

        return out


class ConnectPacket(MQTTPacket):
    VARIABLE_HEADER = ConnectVariableHeader
    PAYLOAD = ConnectPayload

    @property
    def proto_name(self):
        return self.variable_header.proto_name

    @proto_name.setter
    def proto_name(self, name: str):
        self.variable_header.proto_name = name

    @property
    def proto_level(self):
        return self.variable_header.proto_level

    @proto_level.setter
    def proto_level(self, level):
        self.variable_header.proto_level = level

    @property
    def username_flag(self):
        return self.variable_header.username_flag

    @username_flag.setter
    def username_flag(self, flag):
        self.variable_header.username_flag = flag

    @property
    def password_flag(self):
        return self.variable_header.password_flag

    @password_flag.setter
    def password_flag(self, flag):
        self.variable_header.password_flag = flag

    @property
    def clean_session_flag(self):
        return self.variable_header.clean_session_flag

    @clean_session_flag.setter
    def clean_session_flag(self, flag):
        self.variable_header.clean_session_flag = flag

    @property
    def will_retain_flag(self):
        return self.variable_header.will_retain_flag

    @will_retain_flag.setter
    def will_retain_flag(self, flag):
        self.variable_header.will_retain_flag = flag

    @property
    def will_qos(self):
        return self.variable_header.will_qos

    @will_qos.setter
    def will_qos(self, flag):
        self.variable_header.will_qos = flag

    @property
    def will_flag(self):
        return self.variable_header.will_flag

    @will_flag.setter
    def will_flag(self, flag):
        self.variable_header.will_flag = flag

    @property
    def reserved_flag(self):
        return self.variable_header.reserved_flag

    @reserved_flag.setter
    def reserved_flag(self, flag):
        self.variable_header.reserved_flag = flag

    @property
    def client_id(self):
        return self.payload.client_id

    @client_id.setter
    def client_id(self, client_id):
        self.payload.client_id = client_id

    @property
    def client_id_is_random(self) -> bool:
        return self.payload.client_id_is_random

    @client_id_is_random.setter
    def client_id_is_random(self, client_id_is_random: bool):
        self.payload.client_id_is_random = client_id_is_random

    @property
    def will_topic(self):
        return self.payload.will_topic

    @will_topic.setter
    def will_topic(self, will_topic):
        self.payload.will_topic = will_topic

    @property
    def will_message(self):
        return self.payload.will_message

    @will_message.setter
    def will_message(self, will_message):
        self.payload.will_message = will_message

    @property
    def username(self):
        return self.payload.username

    @username.setter
    def username(self, username):
        self.payload.username = username

    @property
    def password(self):
        return self.payload.password

    @password.setter
    def password(self, password):
        self.payload.password = password

    @property
    def keep_alive(self):
        return self.variable_header.keep_alive

    @keep_alive.setter
    def keep_alive(self, keep_alive):
        self.variable_header.keep_alive = keep_alive

    def __init__(self, fixed: MQTTFixedHeader=None, vh: ConnectVariableHeader=None, payload: ConnectPayload=None):
        if fixed is None:
            header = MQTTFixedHeader(CONNECT, 0x00)
        else:
            if fixed.packet_type is not CONNECT:
                raise HBMQTTException("Invalid fixed packet type %s for ConnectPacket init" % fixed.packet_type)
            header = fixed
        super().__init__(header)
        self.variable_header = vh
        self.payload = payload
