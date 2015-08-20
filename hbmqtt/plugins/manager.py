# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

__all__ = ['get_plugin_manager', 'BaseContext', 'PluginManager']

import pkg_resources
import logging
import asyncio
import copy

from collections import namedtuple

Plugin = namedtuple('Plugin', ['name', 'ep', 'object'])

plugins_manager = dict()


def get_plugin_manager(namespace):
    global plugins_manager
    return plugins_manager.get(namespace, None)


class BaseContext:
    def __init__(self, loop=None):
        if loop is not None:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
        self.logger = None


class PluginManager:
    """
    Wraps setuptools Entry point mechanism to provide a basic plugin system.
    Plugins are loaded for a given namespace (group).
    This plugin manager uses coroutines to run plugin call asynchronously in an event queue
    """
    def __init__(self, namespace, context, loop=None):
        global plugins_manager
        if loop is not None:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()

        self.logger = logging.getLogger(namespace)
        if context is None:
            self.context = BaseContext()
        else:
            self.context = context
        self._plugins = []
        self._load_plugins(namespace)
        self._fired_events = []
        plugins_manager[namespace] = self

    @property
    def app_context(self):
        return self.context

    def _load_plugins(self, namespace):
        self.logger.info("Loading plugins for namespace %s" % namespace)
        for ep in pkg_resources.iter_entry_points(group=namespace):
            plugin = self._load_plugin(ep)
            self._plugins.append(plugin)
            self.logger.info(" Plugin %s ready" % plugin.ep.name)

    def _load_plugin(self, ep: pkg_resources.EntryPoint):
        try:
            self.logger.debug(" Loading plugin %s" % ep)
            plugin = ep.load(require=True)
            self.logger.debug(" Initializing plugin %s" % ep)
            plugin_context = copy.copy(self.app_context)
            plugin_context.logger = self.logger.getChild(ep.name)
            obj = plugin(plugin_context)
            return Plugin(ep.name, ep, obj)
        except ImportError as ie:
            self.logger.warn("Plugin %r import failed: %s" % (ep, ie))
        except pkg_resources.UnknownExtra as ue:
            self.logger.warn("Plugin %r dependencies resolution failed: %s" % (ep, ue))

    def get_plugin(self, name):
        """
        Get a plugin by its name from the plugins loaded for the current namespace
        :param name:
        :return:
        """
        for p in self._plugins:
            if p.name == name:
                return p
        return None

    @asyncio.coroutine
    def close(self):
        """
        Free PluginManager resources and cancel pending event methods
        This method call a close() coroutine for each plugin, allowing plugins to close and free resources
        :return:
        """
        yield from self.map_plugin_coro("close")
        for task in self._fired_events:
            task.cancel()

    @property
    def plugins(self):
        """
        Get the loaded plugins list
        :return:
        """
        return self._plugins

    def _schedule_coro(self, coro):
        return asyncio.Task(coro, loop=self._loop)

    @asyncio.coroutine
    def fire_event(self, event_name, wait=False, *args, **kwargs):
        """
        Fire an event to plugins.
        PluginManager schedule async calls for each plugin on method called "on_" + event_name
        For example, on_connect will be called on event 'connect'
        Method calls are schedule in the asyn loop. wait parameter must be set to true to wait until all
        mehtods are completed.
        :param event_name:
        :param args:
        :param kwargs:
        :param wait: indicates if fire_event should wait for plugin calls completion (True), or not
        :return:
        """
        tasks = []
        event_method_name = "on_" + event_name
        for plugin in self._plugins:
            event_method = getattr(plugin.object, event_method_name, None)
            if event_method:
                tasks.append(self._schedule_coro(event_method(*args, **kwargs)))
        if wait:
            if len(tasks) > 0:
                yield from asyncio.wait(tasks, loop=self._loop)
        else:
            self._fired_events.extend(tasks)


    @asyncio.coroutine
    def map(self, coro, *args, **kwargs):
        """
        Schedule a given coroutine call for each plugin.
        The coro called get the Plugin instance as first argument of its method call
        :param coro:
        :param args:
        :param kwargs:
        :return:
        """
        tasks = []
        for plugin in self._plugins:
            coro_instance = coro(plugin, *args, **kwargs)
            if coro_instance:
                tasks.append(self._schedule_coro(coro_instance))
        ret = yield from asyncio.gather(*tasks, loop=self._loop)
        return ret

    @staticmethod
    def _get_coro(plugin, coro_name, *args, **kwargs):
        try:
            return getattr(plugin.object, coro_name, None)(*args, **kwargs)
        except TypeError:
            # Plugin doesn't implement coro_name
            return None

    @asyncio.coroutine
    def map_plugin_coro(self, coro_name, *args, **kwargs):
        """
        Call a plugin declared by plugin by its name
        :param coro_name:
        :param args:
        :param kwargs:
        :return:
        """
        return (yield from self.map(self._get_coro, coro_name, *args, **kwargs))
