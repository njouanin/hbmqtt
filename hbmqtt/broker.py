# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
import threading
import logging
from hbmqtt.errors import BrokerException
from transitions import Machine, MachineError


class BrokerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        pass

    def data_received(self, data):
        pass


class Broker:
    states = ['new', 'starting', 'started', 'not_started', 'stopping', 'stopped', 'not_stopped', 'stopped']

    def __init__(self, host='127.0.0.1', port=1883, loop = None):
        self.logger = logging.getLogger(__name__)
        self._init_states()
        self.host = host
        self.port = port
        if loop is not None:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
        self._server = None
        self._loop_thread = None

    def _init_states(self):
        self.machine = Machine(states=Broker.states, initial='new')
        self.machine.add_transition(trigger='start', source='new', dest='starting')
        self.machine.add_transition(trigger='starting_fail', source='starting', dest='not_started')
        self.machine.add_transition(trigger='starting_success', source='starting', dest='started')
        self.machine.add_transition(trigger='shutdown', source='started', dest='stopping')
        self.machine.add_transition(trigger='stopping_success', source='stopping', dest='stopped')
        self.machine.add_transition(trigger='stopping_failure', source='stopping', dest='not_stopped')
        self.machine.add_transition(trigger='start', source='stopped', dest='starting')

    def start(self):
        try:
            self.machine.start()
            self.logger.debug("Broker starting")
        except MachineError as me:
            self.logger.debug("Invalid method call at this moment: %s" % me)
            raise BrokerException("Broker instance can't be started: %s" % me)

        try:
            self._loop_thread = threading.Thread(target=self._run_server_loop, args=(self._loop,))
            self._loop_thread.setDaemon(True)
            self._loop_thread.start()
        except Exception as e:
            self.logger.error("Broker startup failed: %s" % e)
            self.machine.starting_fail()
            raise BrokerException("Broker instance can't be started: %s" % e)

    def shutdown(self):
        try:
            self.machine.shutdown()
        except MachineError as me:
            self.logger.debug("Invalid method call at this moment: %s" % me)
            raise BrokerException("Broker instance can't be stopped: %s" % me)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._loop_thread.join()
        self._server.close()
        self._loop.run_until_complete(self._server.wait_closed())
        self._loop.close()
        self._server = None
        self.machine.stopping_success()

    def _run_server_loop(self, loop):
        asyncio.set_event_loop(loop)
        coro = loop.create_server(BrokerProtocol, self.host, self.port)
        self._server = loop.run_until_complete(coro)
        self.logger.debug("Broker listening %s:%s" % (self.host, self.port))
        self.machine.starting_success()
        self.logger.debug("Broker started, ready to serve")
        loop.run_forever()