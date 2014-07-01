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

"""There are several places we need to process the streams:
   1. When a new event comes in.
   2. Periodically to check for expired streams.
   3. Periodically to process triggered streams.
   4. Periodically to delete processed streams.
   These last three could be dealing with potentially large sets
   and each operation could be done by multiple workers.
"""


class BadEvent(Exception):
    pass


class Pipeline(object):
    def __init__(self, rules, sync_engine):
        self.sync_engine = sync_engine
        self.rules = rules  # [StreamRule, ...]

    def add_event(self, event):
        # We save the event, but only deal with the
        # message_id during stream processing.
        message_id = event.get('message_id')
        self.sync_engine.save_event(message_id, event)
        if not message_id:
            raise BadEvent("Event has no message_id")

        # An event may apply to many streams ...
        for rule in self.rules:
            stream = rule.get_active_stream(event)
            if stream:
                stream.add_message(message_id)
                rule.should_trigger(stream, event)

    # These methods are called as periodic tasks and
    # may be expensive (in that they may iterate over
    # all streams).
    def do_expiry_check(self, now=None):
        if now is None:
            now = datetime.datetime.utcnow()

        for rule in self.rules:
            rule.expiry_check(now)

    def purge_streams(self):
        self.sync_engine.purge_processed_streams()

    def process_triggered_streams(self, now=None):
        """If the stream is triggered we need to process the
           pipeline.
        """
        if now is None:
            now = datetime.datetime.utcnow()
        for rule in self.rules:
            rule.process_triggered_streams(now)
