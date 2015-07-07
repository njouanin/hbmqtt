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