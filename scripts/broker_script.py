# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
"""
HBMQTT - MQTT 3.1.1 broker

Usage:
    hbmqtt --version
    hbmqtt (-h | --help)
    hbmqtt [-c <config_file> ] [-d]

Options:
    -h --help           Show this screen.
    --version           Show version.
    -c <config_file>    Broker configuration file (YAML format)
    -d                  Enable debug messages
"""

import sys
import logging
import asyncio
import os
from hbmqtt.broker import Broker
from hbmqtt.version import get_version
from docopt import docopt
from hbmqtt.utils import read_yaml_config


default_config = {
    'listeners': {
        'default': {
            'type': 'tcp',
            'bind': '0.0.0.0:1883',
        },
    },
    'sys_interval': 10,
    'auth': {
        'allow-anonymous': True,
        'password-file': os.path.join(os.path.dirname(os.path.realpath(__file__)), "passwd"),
        'plugins': [
            'auth_file', 'auth_anonymous'
        ]
    },
    'topic-check': {
        'enabled': False
    }
}

logger = logging.getLogger(__name__)


def main(*args, **kwargs):
    if sys.version_info[:2] < (3, 4):
        logger.fatal("Error: Python 3.4+ is required")
        sys.exit(-1)

    arguments = docopt(__doc__, version=get_version())
    formatter = "[%(asctime)s] :: %(levelname)s - %(message)s"

    if arguments['-d']:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level, format=formatter)

    config = None
    if arguments['-c']:
        config = read_yaml_config(arguments['-c'])
    else:
        config = read_yaml_config(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'default_broker.yaml'))
        logger.debug("Using default configuration")
    loop = asyncio.get_event_loop()
    broker = Broker(config)
    try:
        loop.run_until_complete(broker.start())
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(broker.shutdown())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
