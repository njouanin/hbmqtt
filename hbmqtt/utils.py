# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.


def not_in_dict_or_none(dict, key):
    """
    Check if a key exists in a map and if it's not None
    :param dict: map to look for key
    :param key: key to find
    :return: true if key is in dict and not None
    """
    if key not in dict or dict[key] is None:
        return True
    else:
        return False


def format_client_message(session=None, address=None, port=None, id=None):
    if session:
        return "(client @=%s:%d id=%s)" % (session.remote_address, session.remote_port, session.client_id)
    else:
        return "(client @=%s:%d id=%s)" % (address, port, id)


def gen_client_id():
    """
    Generates random client ID
    :return:
    """
    import random
    gen_id = 'hbmqtt/'

    for i in range(7, 23):
        gen_id += chr((int(random.random()*1000) % 73) + 48)
    return gen_id
