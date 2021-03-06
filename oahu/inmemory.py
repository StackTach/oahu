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

import db_driver
import stream as pstream


class LocalStream(pstream.Stream):
    def __init__(self, uuid, trigger_name, state, last_update,
                 identifying_traits, inmemory_stream):
        super(LocalStream, self).__init__(uuid, trigger_name,
                                          state, last_update,
                                          identifying_traits)
        self.inmemory_stream = inmemory_stream

    def load_events(self):
        self.events = self.inmemory_stream.messages


class InMemoryStream(object):
    def __init__(self, trigger_name, identifying_traits, event):
        self.trigger_name = trigger_name
        self.sid = str(uuid.uuid4())
        self.messages = []
        self.last_update = datetime.datetime.utcnow()
        self.state = pstream.COLLECTING
        self.last_error = None
        self.commit_errors = 0
        self.events = None

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


class InMemoryDriver(db_driver.DBDriver):
    """All the pipeline operations that need to be externalized
       to support concurrent processing.
    """

    def save_event(self, mid, event):
        self.raw_events[mid] = event

    def append_event(self, message_id, trigger, event, trait_dict):
        stream = None
        streams = self.active_streams.get(trigger.name, {})
        for sid, s in streams.iteritems():
            if s.do_identifying_traits_match(event):
                stream = s
                break

        is_new_stream = False
        if not stream:
            stream = self._create_stream(trigger.name,
                                         trigger.get_identifying_trait_names(),
                                         event)
            is_new_stream = True

        stream.messages.append(message_id)
        now = datetime.datetime.utcnow()
        stream.last_update = now
        self._check_for_trigger(trigger, stream, event=event, now=now)
        return is_new_stream

    def do_trigger_check(self, state, chunk, now=None):
        for trigger in self.trigger_defs:
            for sid, stream in self.active_streams[trigger.name].iteritems():
                self._check_for_trigger(trigger, stream, now=now)

    def purge_processed_streams(self, state, chunk):
        togo = []
        for rid, stream_map in self.active_streams.iteritems():
            for sid, stream in stream_map.iteritems():
                if stream.state == pstream.PROCESSED:
                    togo.append((rid, sid))

        for rid, sid in togo:
            del self.active_streams[rid][sid]

    def process_ready_streams(self, state, chunk, now):
        for trigger in self.trigger_defs:
            for s in self._get_ready_streams(trigger.name):
                stream = LocalStream(s.sid, s.trigger_name, s.state,
                                     s.last_update,
                                     s.identifying_traits, s)
                stream.set_events(self._get_events(s.messages))
                self._do_pipeline_callbacks(stream, trigger)

    def ready(self, trigger_name, stream):
        self._change_stream_state(trigger_name, stream.sid, pstream.READY)

    def trigger(self, trigger_name, stream):
        self._change_stream_state(trigger_name, stream.sid, pstream.TRIGGERED)

    def processed(self, trigger_name, stream):
        self._change_stream_state(trigger_name, stream.sid, pstream.PROCESSED)

    def error(self, trigger_name, stream, error):
        self._change_stream_state(trigger_name, stream.sid, pstream.ERROR)
        self.active_streams[trigger_name][stream.sid].last_error = error

    def commit_error(self, trigger_name, stream, error):
        self._change_stream_state(trigger_name, stream.sid,
                                  pstream.COMMIT_ERROR)
        s = self.active_streams[trigger_name][stream.sid]
        s.last_error = error
        s.commit_errors += 1

    def get_num_active_streams(self, trigger_name):
        return len(self.active_streams.get(trigger_name, {}))

    def find_streams(self, **kwargs):
        return []  # TODO(sandy): need this for tox tests.

    def get_stream(self, stream_id, details):
        for trigger in self.trigger_defs:
            for s in self._get_ready_streams(trigger.name):
                if s.sid == stream_id:
                    result = LocalStream(s.sid, s.trigger_name, s.state,
                                  s.last_update, s.identifying_traits, s)
                    if details:
                        result.load_events()
                    return result.to_dict()

    def flush_all(self):
        # { trigger_name: { stream_id: InMemoryStream } }
        self.active_streams = {}

        # Obviously keeping all these in memory is very
        # expensive. Only suitable for tiny tests.
        self.raw_events = {}  # { message_id: event_dict }

    def _get_events(self, message_ids):
        return [self.raw_events[mid] for mid in message_ids]

    def _get_ready_streams(self, trigger_name):
        streams = []
        for sid, stream in self.active_streams[trigger_name].iteritems():
            if stream.state == pstream.READY:
                streams.append(stream)
        return streams

    def _create_stream(self, trigger_name, identifying_trait_names, event):
        stream = InMemoryStream(trigger_name, identifying_trait_names, event)
        streams = self.active_streams.get(trigger_name, {})
        streams[stream.sid] = stream
        self.active_streams[trigger_name] = streams
        return stream

    def _change_stream_state(self, trigger_name, stream_id, new_state):
        self.active_streams[trigger_name][stream_id].state = new_state
