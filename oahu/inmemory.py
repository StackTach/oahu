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

import sync_engine
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


class InMemorySyncEngine(sync_engine.SyncEngine):
    """All the pipeline operations that need to be externalized
       to support concurrent processing.
    """

    def __init__(self, rules):
        super(InMemorySyncEngine, self).__init__(rules)
        self.flush_all()

    def save_event(self, mid, event):
        self.raw_events[mid] = event

    def append_event(self, message_id, rule, event, trait_dict):
        stream = None
        streams = self.active_streams.get(rule.rule_id, {})
        for sid, s in streams.iteritems():
            if s.do_identifying_traits_match(event):
                stream = s
                break

        if not stream:
            stream = self._create_stream(rule.rule_id,
                                         rule.get_identifying_trait_names(),
                                         event)

        stream.messages.append(message_id)
        now = datetime.datetime.utcnow()
        stream.last_update = now
        self._check_for_trigger(rule, stream, event=event, now=now)

    def do_expiry_check(self, now=None):
        for rule in self.rules:
            for sid, stream in self.active_streams[rule.rule_id].iteritems():
                self._check_for_trigger(rule, stream, now=now)

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
            for s in self._get_triggered_streams(rule.rule_id):
                stream = pstream.Stream(s.sid, rule.rule_id,
                                        s.state, s.last_update)
                stream.set_events(self._get_events(s.messages))
                rule.trigger_callback.on_trigger(stream)
                self._processed(rule.rule_id, s)

    def trigger(self, rule_id, stream):
        self._change_stream_state(rule_id, stream.sid, pstream.TRIGGERED)

    def get_num_active_streams(self, rule_id):
        return len(self.active_streams.get(rule_id, {}))

    def flush_all(self):
        self.active_streams = {}  # { rule_id: { stream_id: InMemoryStream } }

        # Obviously keeping all these in memory is very
        # expensive. Only suitable for tiny tests.
        self.raw_events = {}  # { message_id: event_dict }

    def _get_events(self, message_ids):
        return [self.raw_events[mid] for mid in message_ids]

    def _get_triggered_streams(self, rule_id):
        streams = []
        for sid, stream in self.active_streams[rule_id].iteritems():
            if stream.state == pstream.TRIGGERED:
                streams.append(stream)
        return streams

    def _create_stream(self, rule_id, identifying_trait_names, event):
        stream = InMemoryStream(rule_id, identifying_trait_names, event)
        streams = self.active_streams.get(rule_id, {})
        streams[stream.sid] = stream
        self.active_streams[rule_id] = streams
        return stream

    def _change_stream_state(self, rule_id, stream_id, new_state):
        self.active_streams[rule_id][stream_id].state = new_state

    def _processed(self, rule_id, stream):
        self._change_stream_state(rule_id, stream.sid, pstream.PROCESSED)
