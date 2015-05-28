# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.


def bytes_to_hex_str(data):
    return '0x' + ''.join(format(b, '02x') for b in data)

def hex_to_int(data):
    return int.from_bytes(data, byteorder='big')