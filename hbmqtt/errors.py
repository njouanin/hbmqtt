# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

class BrokerException(BaseException):
    pass

class MQTTException(BaseException):
    """
    Base class for all errors refering to MQTT specifications
    """
    pass

class CodecException(HandlerException):
    """
    Exceptions thrown by packet encode/decode functions
    """
    pass
