# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
from transitions import Machine, MachineError
from datetime import datetime
from hbmqtt.errors import HBMQTTException


class InFlightMessage:
    states = ['new', 'published', 'acknowledged', 'received', 'released', 'completed']

    def __init__(self, packet, qos, ack_timeout=0, loop=None):
        if loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = loop
        self.publish_packet = packet
        self.qos = qos
        self.publish_ts = None
        self.puback_ts = None
        self.pubrec_ts = None
        self.pubrel_ts = None
        self.pubcomp_ts = None
        self.nb_retries = 0
        self._ack_waiter = asyncio.Future(loop=self._loop)
        self._ack_timeout = ack_timeout
        self._ack_timeout_handle = None
        self._init_states()

    def _init_states(self):
        self.machine = Machine(model=self, states=InFlightMessage.states, initial='new')
        self.machine.add_transition(trigger='publish', source='new', dest='published')
        self.machine.add_transition(trigger='publish', source='published', dest='published')
        self.machine.add_transition(trigger='publish', source='received', dest='published')
        self.machine.add_transition(trigger='publish', source='released', dest='published')
        if self.qos == 0x01:
            self.machine.add_transition(trigger='acknowledge', source='published', dest='acknowledged')
        if self.qos == 0x02:
            self.machine.add_transition(trigger='receive', source='published', dest='received')
            self.machine.add_transition(trigger='release', source='received', dest='released')
            self.machine.add_transition(trigger='complete', source='released', dest='completed')
            self.machine.add_transition(trigger='acknowledge', source='completed', dest='acknowledged')

    @asyncio.coroutine
    def wait_acknowledge(self):
        return (yield from self._ack_waiter)

    def start_ack_timeout(self):
        def cb_timeout():
            self._ack_waiter.set_result(False)
        if self._ack_timeout:
            self._ack_timeout_handle = self._loop.call_later(self._ack_timeout, cb_timeout)

    def cancel_ack_timeout(self):
        if self._ack_timeout_handle:
            self._ack_timeout_handle.cancel()

    def reset_ack_timeout(self):
        self.cancel_ack_timeout()
        self.start_ack_timeout()

    def cancel(self):
        if self._ack_waiter and not self._ack_waiter.done():
            self._ack_waiter.cancel()
        self.cancel_ack_timeout()


class OutgoingInFlightMessage(InFlightMessage):
    def received_puback(self):
        try:
            self.acknowledge()
            self.puback_ts = datetime.now()
            self.cancel_ack_timeout()
            self._ack_waiter.set_result(True)
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method received_puback on in-flight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def received_pubrec(self):
        try:
            self.receive()
            self.pubrec_ts = datetime.now()
            self.publish_packet = None  # Discard message
            self.reset_ack_timeout()
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method received_pubrec on in-flight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def received_pubcomp(self):
        try:
            self.complete()
            self.pubcomp_ts = datetime.now()
            self.cancel_ack_timeout()
            self._ack_waiter.set_result(True)
            self.acknowledge()
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method received_pubcomp on in-flight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def sent_pubrel(self):
        try:
            self.release()
            self.pubrel_ts = datetime.now()
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method sent_pubrel on in-flight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def retry_publish(self):
        try:
            self.publish()
            self._ack_waiter = asyncio.Future(loop=self._loop)
            self.nb_retries += 1
            self.publish_ts = datetime.now()
            self.start_ack_timeout()
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method retry_publish on in-flight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def sent_publish(self):
        try:
            self.publish()
            self.publish_ts = datetime.now()
            self.start_ack_timeout()
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method sent_publish on in-flight messages with QOS=%d, state=%s' %
                (self.qos, self.state))


class IncomingInFlightMessage(InFlightMessage):
    def __init__(self, packet, qos, ack_timeout=0, loop=None):
        super().__init__(packet, qos, ack_timeout, loop)
        self._pubrel_waiter = asyncio.Future(loop=self._loop)

    def received_publish(self):
        try:
            self.publish()
            self.publish_ts = datetime.now()
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method received_publish on in-flight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def sent_pubrec(self):
        try:
            self.receive()
            self.pubrec_ts = datetime.now()
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method sent_pubrec on in-flight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def sent_pubcomp(self):
        try:
            self.complete()
            self.pubcomp_ts = datetime.now()
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method sent_pubrec on in-flight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    @asyncio.coroutine
    def wait_pubrel(self):
        return (yield from self._pubrel_waiter)

    def received_pubrel(self):
        try:
            self.release()
            self.pubrel_ts = datetime.now()
            self._pubrel_waiter.set_result(True)
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method received_pubcomp on in-flight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def acknowledge_delivery(self):
        try:
            self._ack_waiter.set_result(True)
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method acknowledge_delivery on in-flight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def cancel(self):
        super().cancel()
        if self._pubrel_waiter and not self._pubrel_waiter.done():
            self._pubrel_waiter.cancel()
