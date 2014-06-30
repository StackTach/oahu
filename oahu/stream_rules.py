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


"""There are several places we need to process the streams:
   1. When a new event comes in.
   2. Periodically to check for expired streams.
   3. Periodically to process triggered streams.
   4. Periodically to delete processed streams.
   These last three could be dealing with potentially large sets
   and each operation could be done by multiple workers.
"""

class StreamRule(object):
    def __init__(self, rule_id, sync, identifying_trait_names, trigger_rule,
                 trigger_callback):
        self.rule_id = rule_id
        self.sync = sync
        self.identifying_trait_names = identifying_trait_names
        self.trigger_rule = trigger_rule
        self.trigger_callback = trigger_callback

    def _applies(self, event):
        """Returns True if this rule applies to the supplied Event.

        The default behavior says: if you have the identifying traits
        then this rule applies. Override for more complex stuff.
        """
        for name in self.identifying_trait_names:
            if name not in event:
                return False
        return True

    def get_active_stream(self, event):
        """Returns the active stream for this Event.
           If no stream exists, but the rule applies to this
           event a new one is created.

           The implications is there is only one
           active stream for this rule. Which means
           you have to select unique identifying_traits.
        """
        if not self._applies(event):
            return None

        for stream in self.sync.get_active_streams(self.rule_id):
            if stream.do_identifying_traits_match(event):
                return stream

        return self.sync.create_stream(self.rule_id,
                                       self.identifying_trait_names, event)

    def should_trigger(self, stream, last_event, now=None):
        """last_event could be None if we're doing a periodic check.
        """
        if self.trigger_rule.should_trigger(stream, last_event, now=now):
            stream.trigger()

    def expiry_check(self, now=None):
        for stream in self.sync.get_active_streams(self.rule_id):
            self.should_trigger(stream, None, now=now)

    def process_triggered_streams(self, now):
        """This method provides a means to check streams without
           an actual event.
        """
        for stream in self.sync.get_triggered_streams(self.rule_id):
            self.trigger_callback.on_trigger(stream)
            stream.processed()
