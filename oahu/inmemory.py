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

import stream as pstream


class InMemorySyncEngine(object):
    """All the pipeline operations that need to be externalized
       to support concurrent processing.
    """

    def __init__(self):
        self.active_streams = {}  # { rule_id: { stream_id: Stream } }

        # Obviously keeping all these in memory is very
        # expensive. Only suitable for tiny tests.
        self.raw_events = {}  # { message_id: event_dict }

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

    def get_active_streams(self, rule_id):
        return self.active_streams.get(rule_id, {}).values()

    def get_triggered_streams(self, rule_id):
        streams = []
        for stream_map in self.active_streams.values():
            for sid, stream in stream_map.iteritems():
                if stream.state == pstream.TRIGGERED:
                    streams.append(stream)
        return streams

    def create_stream(self, rule_id, identifying_trait_names, event):
        stream = pstream.Stream(rule_id, identifying_trait_names, event)
        streams = self.active_streams.get(rule_id, {})
        streams[stream.uuid] = stream
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
