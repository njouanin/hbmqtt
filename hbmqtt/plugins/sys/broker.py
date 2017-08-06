# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from datetime import datetime
from hbmqtt.mqtt.packet import PUBLISH
from hbmqtt.codecs import int_to_bytes_str
import asyncio
import sys
from collections import deque

if sys.version_info < (3, 5):
    from asyncio import async as ensure_future
else:
    from asyncio import ensure_future

DOLLAR_SYS_ROOT = '$SYS/broker/'
STAT_BYTES_SENT = 'bytes_sent'
STAT_BYTES_RECEIVED = 'bytes_received'
STAT_MSG_SENT = 'messages_sent'
STAT_MSG_RECEIVED = 'messages_received'
STAT_PUBLISH_SENT = 'publish_sent'
STAT_PUBLISH_RECEIVED = 'publish_received'
STAT_START_TIME = 'start_time'
STAT_CLIENTS_MAXIMUM = 'clients_maximum'
STAT_CLIENTS_CONNECTED = 'clients_connected'
STAT_CLIENTS_DISCONNECTED = 'clients_disconnected'


class BrokerSysPlugin:
    def __init__(self, context):
        self.context = context
        # Broker statistics initialization
        self._stats = dict()
        self._sys_handle = None

    def _clear_stats(self):
        """
        Initializes broker statistics data structures
        """
        for stat in (STAT_BYTES_RECEIVED,
                     STAT_BYTES_SENT,
                     STAT_MSG_RECEIVED,
                     STAT_MSG_SENT,
                     STAT_CLIENTS_MAXIMUM,
                     STAT_CLIENTS_CONNECTED,
                     STAT_CLIENTS_DISCONNECTED,
                     STAT_PUBLISH_RECEIVED,
                     STAT_PUBLISH_SENT):
            self._stats[stat] = 0

    @asyncio.coroutine
    def _broadcast_sys_topic(self, topic_basename, data):
        return (yield from self.context.broadcast_message(topic_basename, data))

    def schedule_broadcast_sys_topic(self, topic_basename, data):
        return ensure_future(self._broadcast_sys_topic(DOLLAR_SYS_ROOT + topic_basename, data),
                             loop=self.context.loop)

    @asyncio.coroutine
    def on_broker_pre_start(self, *args, **kwargs):
        self._clear_stats()

    @asyncio.coroutine
    def on_broker_post_start(self, *args, **kwargs):
        self._stats[STAT_START_TIME] = datetime.now()
        from hbmqtt.version import get_version
        version = 'HBMQTT version ' + get_version()
        self.context.retain_message(DOLLAR_SYS_ROOT + 'version', version.encode())

        # Start $SYS topics management
        try:
            sys_interval = int(self.context.config.get('sys_interval', 0))
            if sys_interval > 0:
                self.context.logger.debug("Setup $SYS broadcasting every %d secondes" % sys_interval)
                self.sys_handle = self.context.loop.call_later(sys_interval, self.broadcast_dollar_sys_topics)
            else:
                self.context.logger.debug("$SYS disabled")
        except KeyError:
            pass
            # 'sys_internal' config parameter not found

    @asyncio.coroutine
    def on_broker_pre_stop(self, *args, **kwargs):
        # Stop $SYS topics broadcasting
        if self.sys_handle:
            self.sys_handle.cancel()

    def broadcast_dollar_sys_topics(self):
        """
        Broadcast dynamic $SYS topics updates and reschedule next execution depending on 'sys_interval' config
        parameter.
        """

        # Update stats
        uptime = datetime.now() - self._stats[STAT_START_TIME]
        client_connected = self._stats[STAT_CLIENTS_CONNECTED]
        client_disconnected = self._stats[STAT_CLIENTS_DISCONNECTED]
        inflight_in = 0
        inflight_out = 0
        messages_stored = 0
        for session in self.context.sessions:
            inflight_in += session.inflight_in_count
            inflight_out += session.inflight_out_count
            messages_stored += session.retained_messages_count
        messages_stored += len(self.context.retained_messages)
        subscriptions_count = 0
        for topic in self.context.subscriptions:
            subscriptions_count += len(self.context.subscriptions[topic])

        # Broadcast updates
        tasks = deque()
        tasks.append(self.schedule_broadcast_sys_topic('load/bytes/received', int_to_bytes_str(self._stats[STAT_BYTES_RECEIVED])))
        tasks.append(self.schedule_broadcast_sys_topic('load/bytes/sent', int_to_bytes_str(self._stats[STAT_BYTES_SENT])))
        tasks.append(self.schedule_broadcast_sys_topic('messages/received', int_to_bytes_str(self._stats[STAT_MSG_RECEIVED])))
        tasks.append(self.schedule_broadcast_sys_topic('messages/sent', int_to_bytes_str(self._stats[STAT_MSG_SENT])))
        tasks.append(self.schedule_broadcast_sys_topic('time', str(datetime.now()).encode('utf-8')))
        tasks.append(self.schedule_broadcast_sys_topic('uptime', int_to_bytes_str(int(uptime.total_seconds()))))
        tasks.append(self.schedule_broadcast_sys_topic('uptime/formated', str(uptime).encode('utf-8')))
        tasks.append(self.schedule_broadcast_sys_topic('clients/connected', int_to_bytes_str(client_connected)))
        tasks.append(self.schedule_broadcast_sys_topic('clients/disconnected', int_to_bytes_str(client_disconnected)))
        tasks.append(self.schedule_broadcast_sys_topic('clients/maximum', int_to_bytes_str(self._stats[STAT_CLIENTS_MAXIMUM])))
        tasks.append(self.schedule_broadcast_sys_topic('clients/total', int_to_bytes_str(client_connected + client_disconnected)))
        tasks.append(self.schedule_broadcast_sys_topic('messages/inflight', int_to_bytes_str(inflight_in + inflight_out)))
        tasks.append(self.schedule_broadcast_sys_topic('messages/inflight/in', int_to_bytes_str(inflight_in)))
        tasks.append(self.schedule_broadcast_sys_topic('messages/inflight/out', int_to_bytes_str(inflight_out)))
        tasks.append(self.schedule_broadcast_sys_topic('messages/inflight/stored', int_to_bytes_str(messages_stored)))
        tasks.append(self.schedule_broadcast_sys_topic('messages/publish/received', int_to_bytes_str(self._stats[STAT_PUBLISH_RECEIVED])))
        tasks.append(self.schedule_broadcast_sys_topic('messages/publish/sent', int_to_bytes_str(self._stats[STAT_PUBLISH_SENT])))
        tasks.append(self.schedule_broadcast_sys_topic('messages/retained/count', int_to_bytes_str(len(self.context.retained_messages))))
        tasks.append(self.schedule_broadcast_sys_topic('messages/subscriptions/count', int_to_bytes_str(subscriptions_count)))

        # Wait until broadcasting tasks end
        while tasks and tasks[0].done():
            tasks.popleft()
        # Reschedule
        sys_interval = int(self.context.config['sys_interval'])
        self.context.logger.debug("Broadcasting $SYS topics")
        self.sys_handle = self.context.loop.call_later(sys_interval, self.broadcast_dollar_sys_topics)

    @asyncio.coroutine
    def on_mqtt_packet_received(self, *args, **kwargs):
        packet = kwargs.get('packet')
        if packet:
            packet_size = packet.bytes_length
            self._stats[STAT_BYTES_RECEIVED] += packet_size
            self._stats[STAT_MSG_RECEIVED] += 1
            if packet.fixed_header.packet_type == PUBLISH:
                self._stats[STAT_PUBLISH_RECEIVED] += 1

    @asyncio.coroutine
    def on_mqtt_packet_sent(self, *args, **kwargs):
        packet = kwargs.get('packet')
        if packet:
            packet_size = packet.bytes_length
            self._stats[STAT_BYTES_SENT] += packet_size
            self._stats[STAT_MSG_SENT] += 1
            if packet.fixed_header.packet_type == PUBLISH:
                self._stats[STAT_PUBLISH_SENT] += 1

    @asyncio.coroutine
    def on_broker_client_connected(self, *args, **kwargs):
        self._stats[STAT_CLIENTS_CONNECTED] += 1
        self._stats[STAT_CLIENTS_MAXIMUM] = max(self._stats[STAT_CLIENTS_MAXIMUM], self._stats[STAT_CLIENTS_CONNECTED])

    @asyncio.coroutine
    def on_broker_client_disconnected(self, *args, **kwargs):
        self._stats[STAT_CLIENTS_CONNECTED] -= 1
        self._stats[STAT_CLIENTS_DISCONNECTED] += 1
