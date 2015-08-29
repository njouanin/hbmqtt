# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

import unittest
import logging
import os
import asyncio
from hbmqtt.plugins.manager import BaseContext
from hbmqtt.plugins.authentication import AnonymousAuthPlugin, FileAuthPlugin
from hbmqtt.session import Session

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=formatter)


class TestAnonymousAuthPlugin(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_allow_anonymous(self):
        context = BaseContext()
        context.logger = logging.getLogger(__name__)
        context.config = {
            'auth': {
                'allow-anonymous': True
            }
        }
        s = Session()
        s.username = ""
        auth_plugin = AnonymousAuthPlugin(context)
        ret = self.loop.run_until_complete(auth_plugin.authenticate(session=s))
        self.assertTrue(ret)

    def test_disallow_anonymous(self):
        context = BaseContext()
        context.logger = logging.getLogger(__name__)
        context.config = {
            'auth': {
                'allow-anonymous': False
            }
        }
        s = Session()
        s.username = ""
        auth_plugin = AnonymousAuthPlugin(context)
        ret = self.loop.run_until_complete(auth_plugin.authenticate(session=s))
        self.assertFalse(ret)

    def test_allow_nonanonymous(self):
        context = BaseContext()
        context.logger = logging.getLogger(__name__)
        context.config = {
            'auth': {
                'allow-anonymous': False
            }
        }
        s = Session()
        s.username = "test"
        auth_plugin = AnonymousAuthPlugin(context)
        ret = self.loop.run_until_complete(auth_plugin.authenticate(session=s))
        self.assertTrue(ret)


class TestFileAuthPlugin(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_allow(self):
        context = BaseContext()
        context.logger = logging.getLogger(__name__)
        context.config = {
            'auth': {
                'password-file': os.path.join(os.path.dirname(os.path.realpath(__file__)), "passwd")
            }
        }
        s = Session()
        s.username = "user"
        s.password = "test"
        auth_plugin = FileAuthPlugin(context)
        ret = self.loop.run_until_complete(auth_plugin.authenticate(session=s))
        self.assertTrue(ret)

    def test_wrong_password(self):
        context = BaseContext()
        context.logger = logging.getLogger(__name__)
        context.config = {
            'auth': {
                'password-file': os.path.join(os.path.dirname(os.path.realpath(__file__)), "passwd")
            }
        }
        s = Session()
        s.username = "user"
        s.password = "wrong password"
        auth_plugin = FileAuthPlugin(context)
        ret = self.loop.run_until_complete(auth_plugin.authenticate(session=s))
        self.assertFalse(ret)

    def test_unknown_password(self):
        context = BaseContext()
        context.logger = logging.getLogger(__name__)
        context.config = {
            'auth': {
                'password-file': os.path.join(os.path.dirname(os.path.realpath(__file__)), "passwd")
            }
        }
        s = Session()
        s.username = "some user"
        s.password = "some password"
        auth_plugin = FileAuthPlugin(context)
        ret = self.loop.run_until_complete(auth_plugin.authenticate(session=s))
        self.assertFalse(ret)
