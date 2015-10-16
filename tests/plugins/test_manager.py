# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import logging
import asyncio
from hbmqtt.plugins.manager import PluginManager, BaseContext

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=formatter)


class TestPlugin:
    def __init__(self, context):
        self.context = context


class EventTestPlugin:
    def __init__(self, context):
        self.context = context
        self.test_flag = False
        self.coro_flag = False

    async def on_test(self, *args, **kwargs):
        self.test_flag = True
        self.context.logger.info("on_test")

    async def test_coro(self, *args, **kwargs):
        self.coro_flag = True

    async def ret_coro(self, *args, **kwargs):
        return "TEST"


class TestPluginManager(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_load_plugin(self):
        manager = PluginManager("hbmqtt.test.plugins", context=None)
        self.assertTrue(len(manager._plugins) > 0)

    def test_fire_event(self):
        async def fire_event():
            await manager.fire_event("test")
            await asyncio.sleep(1, loop=self.loop)
            await manager.close()

        manager = PluginManager("hbmqtt.test.plugins", context=None, loop=self.loop)
        self.loop.run_until_complete(fire_event())
        plugin = manager.get_plugin("event_plugin")
        self.assertTrue(plugin.object.test_flag)

    def test_fire_event_wait(self):
        async def fire_event():
            await manager.fire_event("test", wait=True)
            await manager.close()

        manager = PluginManager("hbmqtt.test.plugins", context=None, loop=self.loop)
        self.loop.run_until_complete(fire_event())
        plugin = manager.get_plugin("event_plugin")
        self.assertTrue(plugin.object.test_flag)

    def test_map_coro(self):
        async def call_coro():
            await manager.map_plugin_coro('test_coro')

        manager = PluginManager("hbmqtt.test.plugins", context=None, loop=self.loop)
        self.loop.run_until_complete(call_coro())
        plugin = manager.get_plugin("event_plugin")
        self.assertTrue(plugin.object.test_coro)

    def test_map_coro_return(self):
        async def call_coro():
            return await manager.map_plugin_coro('ret_coro')

        manager = PluginManager("hbmqtt.test.plugins", context=None, loop=self.loop)
        ret = self.loop.run_until_complete(call_coro())
        plugin = manager.get_plugin("event_plugin")
        self.assertEqual(ret[plugin], "TEST")

    def test_map_coro_filter(self):
        """
        Run plugin coro but expect no return as an empty filter is given
        :return:
        """
        async def call_coro():
            return await manager.map_plugin_coro('ret_coro', filter_plugins=[])

        manager = PluginManager("hbmqtt.test.plugins", context=None, loop=self.loop)
        ret = self.loop.run_until_complete(call_coro())
        self.assertTrue(len(ret) == 0)
