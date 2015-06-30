# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

from setuptools import setup, find_packages
from hbmqtt.version import get_version

setup(
    name="hbmqtt",
    version=get_version(),
    description="HBMQTT - HomeBrew MQTT\nclient/brocker using Python 3.4 asyncio library",
    author="Nicolas Jouanin",
    author_email='nico@beerfactory.org',
    url="https://github.com/beerfactory/hbmqtt",
    license='MIT',
    packages=find_packages(exclude=['tests']),
    install_requires=['transitions', 'blinker'],
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
    ]
)