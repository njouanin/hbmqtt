# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

class HBMQTTException(BaseException):
    """
    HBMQTT base exception
    """
    pass

class MQTTException(HBMQTTException):
    """
    Base class for all errors refering to MQTT specifications
    """
    pass

class CodecException(HBMQTTException):
    """
    Exceptions thrown by packet encode/decode functions
    """
    pass

class NoDataException(HBMQTTException):
    """
    Exceptions thrown by packet encode/decode functions
    """
    pass
