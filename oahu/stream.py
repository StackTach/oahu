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


COLLECTING = 1
READY = 2
TRIGGERED = 3
PROCESSED = 4

readable = {COLLECTING: "Collecting",
            READY: "Ready",
            TRIGGERED: "Triggered",
            PROCESSED: "Processed"}


class Stream(object):
    # ORM-like object for the Stream. Instances of this class will come
    # and go as the sync-engine needs them.
    #
    # So ... keep any important state change operations out of here.
    # It's likely the state will change via another worker.

    def __init__(self, uuid, rule_id, state, last_update):
        self.uuid = uuid
        self.rule_id = rule_id
        self.last_update = last_update
        self.state = state
        self.events = None  # Lazy loaded for stream processing only.

    def set_events(self, events):
        self.events = events

    def __str__(self):
        return "<Stream %s: Rule '%s' - %s>" % (self.uuid,
                                              self.rule_id,
                                              readable[self.state])
