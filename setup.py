# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

from setuptools import setup, find_packages
from hbmqtt.version import get_version

setup(
    name="hbmqtt",
    version=get_version(),
    description="MQTT client/brocker using Python 3.4 asyncio library",
    author="Nicolas Jouanin",
    author_email='nico@beerfactory.org',
    url="https://github.com/beerfactory/hbmqtt",
    license='MIT',
    packages=find_packages(exclude=['tests']),
    platforms='all',
    install_requires=[
        'transitions==0.2.5',
        'websockets',
        'passlib',
        'docopt',
        'pyyaml'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python :: 3.4',
        'Topic :: Communications',
        'Topic :: Internet'
    ],
    entry_points = {
        'hbmqtt.test.plugins': [
            'test_plugin = tests.plugins.test_manager:TestPlugin',
            'event_plugin = tests.plugins.test_manager:EventTestPlugin',
            'packet_logger_plugin = hbmqtt.plugins.logging:PacketLoggerPlugin',
        ],
        'hbmqtt.broker.plugins': [
#            'event_logger_plugin = hbmqtt.plugins.logging:EventLoggerPlugin',
            'packet_logger_plugin = hbmqtt.plugins.logging:PacketLoggerPlugin',
            'auth_anonymous = hbmqtt.plugins.authentication:AnonymousAuthPlugin',
            'auth_file = hbmqtt.plugins.authentication:FileAuthPlugin',
            'broker_sys = hbmqtt.plugins.sys.broker:BrokerSysPlugin',
        ],
        'hbmqtt.client.plugins': [
            'packet_logger_plugin = hbmqtt.plugins.logging:PacketLoggerPlugin',
        ],
        'console_scripts': [
            'hbmqtt = scripts.hbmqtt:main',
            'hbmqtt_pub = scripts.hbmqtt_pub:main',
            'hbmqtt_sub = scripts.hbmqtt_sub:main',
        ]
    }
)