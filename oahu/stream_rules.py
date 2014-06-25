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

class StreamRule(object):
    def __init__(self, identifying_trait_names, trigger_rule,
                 trigger_callback):
        self.active_streams = {}   # { stream_id: Stream }
        self.identifying_trait_names = identifying_trait_names
        self.trigger_rule = trigger_rule
        self.trigger_callback = trigger_callback
        self.streams_to_purge = []

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

        for sid, stream in self.active_streams.iteritems():
            if stream.do_identifying_traits_match(event):
                return stream

        # New stream ...
        stream = pstream.Stream(self.identifying_trait_names, event)
        self.active_streams[stream.uuid] = stream
        return stream

    def should_trigger(self, stream, last_event, now=None):
        """last_event could be None if we're doing a periodic check.
        """
        if self.trigger_rule.should_trigger(stream, last_event, now=now):
            self.trigger_callback.on_trigger(stream)
            self.streams_to_purge.append(stream.uuid)

    def purge_streams(self):
        for sid in self.streams_to_purge:
            del self.active_streams[sid]
        self.streams_to_purge = []

    def do_expiry_check(self, now):
        """This method provides a means to check streams without
           an actual event.
        """
        for sid, stream in self.active_streams.iteritems():
            self.should_trigger(stream, None, now=now)

        self.purge_streams()
