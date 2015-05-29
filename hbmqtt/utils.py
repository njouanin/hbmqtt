# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
from hbmqtt.streams.errors import NoDataException

def bytes_to_hex_str(data):
    return '0x' + ''.join(format(b, '02x') for b in data)

def hex_to_int(data):
    return int.from_bytes(data, byteorder='big')

@asyncio.coroutine
def read_or_raise(reader, n=-1):
    data = yield from reader.read(n)
    if not data:
        raise NoDataException
    return data

@asyncio.coroutine
def read_string(reader):
    length = yield from read_or_raise(reader, 2)