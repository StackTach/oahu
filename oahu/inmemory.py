# Copyright (c) 2014 Dark Secret Software Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import uuid

import stream as pstream


class InMemoryStream(object):
    def __init__(self, rule_id, identifying_traits, event):
        self.rule_id = rule_id
        self.sid = str(uuid.uuid4())
        self.messages = []
        self.last_update = datetime.datetime.utcnow()
        self.state = pstream.COLLECTING

        # Don't do this if we're creating from an existing stream ...
        self._extract_identifying_traits(identifying_traits, event)

    def _extract_identifying_traits(self, it, event):
        self.identifying_traits = {}  # { trait: value }
        for name in it:
            self.identifying_traits[name] = event[name]

    def do_identifying_traits_match(self, event):
        for name, value in self.identifying_traits.iteritems():
            if event.get(name) != self.identifying_traits[name]:
                return False
        return True


class InMemorySyncEngine(object):
    """All the pipeline operations that need to be externalized
       to support concurrent processing.
    """

    def __init__(self, rules):
        self.active_streams = {}  # { rule_id: { stream_id: InMemoryStream } }
        self.rules = rules  # [StreamRule, ...]

        # Obviously keeping all these in memory is very
        # expensive. Only suitable for tiny tests.
        self.raw_events = {}  # { message_id: event_dict }

    def add_event(self, event):
        # We save the event, but only deal with the
        # message_id during stream processing.
        message_id = event.get('message_id')
        if not message_id:
            raise BadEvent("Event has no message_id")
        self.save_event(message_id, event)

        # An event may apply to many streams ...
        for rule in self.rules:
            if not rule.applies(event):
                continue

            stream = None
            streams = self.active_streams.get(rule.rule_id, {})
            for sid, s in streams.iteritems():
                if s.do_identifying_traits_match(event):
                    stream = s
                    break

            if not stream:
                stream = self.create_stream(rule.rule_id,
                                            rule.get_identifying_trait_names(),
                                            event)

            stream.messages.append(message_id)
            now = datetime.datetime.utcnow()
            stream.last_update = now
            self.check_for_trigger(rule, stream, event, now)

    def check_for_trigger(self, rule, stream, event, now):
        if stream.state != pstream.COLLECTING:
            return
        if rule.should_trigger(stream, event, now=now):
            self.trigger(rule.rule_id, stream)

    def lock_stream(self, stream_id):
        pass

    def unlock_stream(self, stream_id):
        pass

    def get_events(self, message_ids):
        return [self.raw_events[mid] for mid in message_ids]

    def save_event(self, mid, event):
        self.raw_events[mid] = event

    def change_stream_state(self, rule_id, stream_id, new_state):
        self.active_streams[rule_id][stream_id].state = new_state

    def get_triggered_streams(self, rule_id):
        streams = []
        for sid, stream in self.active_streams[rule_id].iteritems():
            if stream.state == pstream.TRIGGERED:
                streams.append(stream)
        return streams

    def do_expiry_check(self, now=None):
        for rule in self.rules:
            for sid, stream in self.active_streams[rule.rule_id].iteritems():
                self.check_for_trigger(rule, stream, None, now)

    def create_stream(self, rule_id, identifying_trait_names, event):
        stream = InMemoryStream(rule_id, identifying_trait_names, event)
        streams = self.active_streams.get(rule_id, {})
        streams[stream.sid] = stream
        self.active_streams[rule_id] = streams
        return stream

    def purge_processed_streams(self):
        togo = []
        for rid, stream_map in self.active_streams.iteritems():
            for sid, stream in stream_map.iteritems():
                if stream.state == pstream.PROCESSED:
                    togo.append((rid, sid))

        for rid, sid in togo:
            del self.active_streams[rid][sid]

    def process_triggered_streams(self, now):
        for rule in self.rules:
            for s in self.get_triggered_streams(rule.rule_id):
                stream = pstream.Stream(s.sid, rule.rule_id,
                                        s.state, s.last_update)
                stream.set_events(self.get_events(s.messages))
                rule.trigger_callback.on_trigger(stream)
                self.processed(rule.rule_id, s)

    def trigger(self, rule_id, stream):
        self.change_stream_state(rule_id, stream.sid, pstream.TRIGGERED)

    def processed(self, rule_id, stream):
        self.change_stream_state(rule_id, stream.sid, pstream.PROCESSED)
