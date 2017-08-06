# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging

import yaml


logger = logging.getLogger(__name__)


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


def format_client_message(session=None, address=None, port=None):
    if session:
        return "(client id=%s)" % session.client_id
    elif address is not None and port is not None:
        return "(client @=%s:%d)" % (address, port)
    else:
        return "(unknown client)"


def gen_client_id():
    """
    Generates random client ID
    :return:
    """
    import random
    gen_id = 'hbmqtt/'

    for i in range(7, 23):
        gen_id += chr(random.randint(0, 74) + 48)
    return gen_id


def read_yaml_config(config_file):
    config = None
    try:
        with open(config_file, 'r') as stream:
            config = yaml.load(stream)
    except yaml.YAMLError as exc:
        logger.error("Invalid config_file %s: %s" % (config_file, exc))
    return config
