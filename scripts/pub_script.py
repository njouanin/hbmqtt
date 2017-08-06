# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
"""
hbmqtt_pub - MQTT 3.1.1 publisher

Usage:
    hbmqtt_pub --version
    hbmqtt_pub (-h | --help)
    hbmqtt_pub --url BROKER_URL -t TOPIC (-f FILE | -l | -m MESSAGE | -n | -s) [-c CONFIG_FILE] [-i CLIENT_ID] [-q | --qos QOS] [-d] [-k KEEP_ALIVE] [--clean-session] [--ca-file CAFILE] [--ca-path CAPATH] [--ca-data CADATA] [ --will-topic WILL_TOPIC [--will-message WILL_MESSAGE] [--will-qos WILL_QOS] [--will-retain] ] [-r]

Options:
    -h --help           Show this screen.
    --version           Show version.
    --url BROKER_URL    Broker connection URL (musr conform to MQTT URI scheme (see https://github.com/mqtt/mqtt.github.io/wiki/URI-Scheme>)
    -c CONFIG_FILE      Broker configuration file (YAML format)
    -i CLIENT_ID        Id to use as client ID.
    -q | --qos QOS      Quality of service to use for the message, from 0, 1 and 2. Defaults to 0.
    -r                  Set retain flag on connect
    -t TOPIC            Message topic
    -m MESSAGE          Message data to send
    -f FILE             Read file by line and publish message for each line
    -s                  Read from stdin and publish message for each line
    -k KEEP_ALIVE       Keep alive timeout in second
    --clean-session     Clean session on connect (defaults to False)
    --ca-file CAFILE]   CA file
    --ca-path CAPATH]   CA Path
    --ca-data CADATA    CA data
    --will-topic WILL_TOPIC
    --will-message WILL_MESSAGE
    --will-qos WILL_QOS
    --will-retain
    -d                  Enable debug messages
"""

import sys
import logging
import asyncio
import os
from hbmqtt.client import MQTTClient, ConnectException
from hbmqtt.version import get_version
from docopt import docopt
from hbmqtt.utils import read_yaml_config

if sys.version_info < (3, 5):
    from asyncio import async as ensure_future
else:
    from asyncio import ensure_future

logger = logging.getLogger(__name__)


def _gen_client_id():
    import os
    import socket
    pid = os.getpid()
    hostname = socket.gethostname()
    return "hbmqtt_pub/%d-%s" % (pid, hostname)


def _get_qos(arguments):
    try:
        return int(arguments['--qos'][0])
    except:
        return None


def _get_message(arguments):
    if arguments['-n']:
        yield b''
    if arguments['-m']:
        yield arguments['-m'].encode(encoding='utf-8')
    if arguments['-f']:
        try:
            with open(arguments['-f'], 'r') as f:
                for line in f:
                    yield line.encode(encoding='utf-8')
        except:
            logger.error("Failed to read file '%s'" % arguments['-f'])
    if arguments['-l']:
        import sys
        for line in sys.stdin:
            if line:
                yield line.encode(encoding='utf-8')
    if arguments['-s']:
        import sys
        message = bytearray()
        for line in sys.stdin:
            message.extend(line.encode(encoding='utf-8'))
        yield message


@asyncio.coroutine
def do_pub(client, arguments):
    running_tasks = []

    try:
        logger.info("%s Connecting to broker" % client.client_id)

        yield from client.connect(uri=arguments['--url'],
                                  cleansession=arguments['--clean-session'],
                                  cafile=arguments['--ca-file'],
                                  capath=arguments['--ca-path'],
                                  cadata=arguments['--ca-data'])
        qos = _get_qos(arguments)
        topic = arguments['-t']
        retain = arguments['-r']
        for message in _get_message(arguments):
            logger.info("%s Publishing to '%s'" % (client.client_id, topic))
            task = ensure_future(client.publish(topic, message, qos, retain))
            running_tasks.append(task)
        if running_tasks:
            yield from asyncio.wait(running_tasks)
        yield from client.disconnect()
        logger.info("%s Disconnected from broker" % client.client_id)
    except KeyboardInterrupt:
        yield from client.disconnect()
        logger.info("%s Disconnected from broker" % client.client_id)
    except ConnectException as ce:
        logger.fatal("connection to '%s' failed: %r" % (arguments['--url'], ce))
    except asyncio.CancelledError as cae:
        logger.fatal("Publish canceled due to prvious error")


def main(*args, **kwargs):
    if sys.version_info[:2] < (3, 4):
        logger.fatal("Error: Python 3.4+ is required")
        sys.exit(-1)

    arguments = docopt(__doc__, version=get_version())
    #print(arguments)
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
        config = read_yaml_config(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'default_client.yaml'))
        logger.debug("Using default configuration")
    loop = asyncio.get_event_loop()

    client_id = arguments.get("-i", None)
    if not client_id:
        client_id = _gen_client_id()

    if arguments['-k']:
        config['keep_alive'] = int(arguments['-k'])

    if arguments['--will-topic'] and arguments['--will-message'] and arguments['--will-qos']:
        config['will'] = dict()
        config['will']['topic'] = arguments['--will-topic']
        config['will']['message'] = arguments['--will-message'].encode('utf-8')
        config['will']['qos'] = int(arguments['--will-qos'])
        config['will']['retain'] = arguments['--will-retain']

    client = MQTTClient(client_id=client_id, config=config, loop=loop)
    loop.run_until_complete(do_pub(client, arguments))
    loop.close()


if __name__ == "__main__":
    main()
