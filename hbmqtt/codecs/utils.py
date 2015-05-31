# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
from asyncio import IncompleteReadError
from hbmqtt.codecs.errors import NoDataException
from math import ceil

def bytes_to_hex_str(data):
    """
    converts a sequence of bytes into its displayable hex representation, ie: 0x??????
    :param data: byte sequence
    :return: Hexadecimal displayable representation
    """
    return '0x' + ''.join(format(b, '02x') for b in data)

def bytes_to_int(data):
    """
    convert a sequence of bytes to an integer using big endian byte ordering
    :param data: byte sequence
    :return: integer value
    """
    return int.from_bytes(data, byteorder='big')

def int_to_bytes(int_value: int) -> bytes:
    """
    convert an integer to a sequence of bytes using big endian byte ordering
    :param int_value: integer value to convert
    :return: byte sequence
    """
    byte_length = ceil(int_value.bit_length()//8)
    if byte_length == 0:
        byte_length = 1
    return int_value.to_bytes(byte_length, byteorder='big')


@asyncio.coroutine
def read_or_raise(reader, n=-1):
    """
    Read a given byte number from Stream. NoDataException is raised if read gives no data
    :param reader: Stream reader
    :param n: number of bytes to read
    :return: bytes read
    """
    try:
        data = yield from reader.readexactly(n)
    except IncompleteReadError:
        raise NoDataException("Incomplete read")
    if not data:
        raise NoDataException
    return data

@asyncio.coroutine
def decode_string(reader):
    """
    Read a string from a reader and decode it according to MQTT string specification
    :param reader: Stream reader
    :return: UTF-8 string read from stream
    """
    length_bytes = yield from read_or_raise(reader, 2)
    str_length = bytes_to_int(length_bytes)
    byte_str = yield from read_or_raise(reader, str_length)
    return byte_str.decode(encoding='utf-8')