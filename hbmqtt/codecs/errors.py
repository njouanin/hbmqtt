# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from hbmqtt.errors import MQTTException
class CodecException(MQTTException):
    """
    Exceptions thrown by encode/decode process
    """
    pass

class NoDataException(CodecException):
    pass
