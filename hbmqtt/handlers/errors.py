class HandlerException(BaseException):
    """
    Exceptions thrown by packet handlers
    """
    pass

class CodecException(HandlerException):
    """
    Exceptions thrown by encode/decode handlers phases
    """
    pass

class NoDataException(HandlerException):
    pass
