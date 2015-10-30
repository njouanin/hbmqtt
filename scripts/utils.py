# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import yaml
import logging

logger = logging.getLogger(__name__)


def read_yaml_config(config_file):
    config = None
    try:
        with open(config_file, 'r') as stream:
            config = yaml.load(stream)
    except yaml.YAMLError as exc:
        logger.error("Invalid config_file %s: %s" % (config_file, exc))
    return config
