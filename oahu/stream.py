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


COLLECTING = 1
TRIGGERED = 2
PROCESSED = 3

readable = {COLLECTING: "Collecting",
            TRIGGERED: "Triggered",
            PROCESSED: "Processed"}


class Stream(object):
    # ORM-like object for the Stream. Instances of this class will come
    # and go as the sync-engine needs them.

    def __init__(self, rule_id, identifying_traits, event, state=COLLECTING):
        self.message_ids = []
        self.uuid = str(uuid.uuid4())
        self.rule_id = rule_id
        self.last_update = datetime.datetime.utcnow()
        self.state = state
        self.events = None

        # Don't do this if we're creating from an existing stream ...
        self._extract_identifying_traits(identifying_traits, event)

    def add_message(self, message_id):
        self.message_ids.append(message_id)
        self.last_update = datetime.datetime.utcnow()

    def _extract_identifying_traits(self, it, event):
        self.identifying_traits = {}  # { trait: value }
        for name in it:
            self.identifying_traits[name] = event[name]

    def do_identifying_traits_match(self, event):
        for name, value in self.identifying_traits.iteritems():
            if event.get(name) != self.identifying_traits[name]:
                return False
        return True

    def load_events(self, sync_engine):
        self.events  = sync_engine.get_events(self.message_ids)

    def __str__(self):
        return "<Stream %s: Rule %s, %d elements - %s>" % (self.uuid,
                                                  self.rule_id,
                                                  len(self.message_ids),
                                                  self.last_update)

    def trigger(self, sync_engine):
        sync_engine.change_stream_state(self.rule_id, self.uuid, TRIGGERED)
        self.state = TRIGGERED

    def processed(self, sync_engine):
        sync_engine.change_stream_state(self.rule_id, self.uuid, PROCESSED)
        self.state = PROCESSED
