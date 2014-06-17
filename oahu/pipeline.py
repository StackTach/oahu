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


class BadEvent(Exception):
    pass


class Stream(object):
    def __init__(self, identifying_traits, event):
        self.message_ids = []
        self.uuid = str(uuid.uuid4())
        self.last_update = datetime.datetime.utcnow()
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


class StreamRule(object):
    def __init__(self, identifying_trait_names, trigger_rule,
                 trigger_callback):
        self.active_streams = {}   # { stream_id: Stream }
        self.identifying_trait_names = identifying_trait_names
        self.trigger_rule = trigger_rule
        self.trigger_callback = trigger_callback

    def _applies(self, event):
        """Returns True if this rule applies to the supplied Event."""
        return True

    def get_active_stream(self, event):
        """Returns the active stream for this Event.
           If no stream exists, but the rule applies to this
           event a new one is created.
        """
        if not self._applies(event):
            return None

        for sid, stream in self.active_streams.iteritems():
            if stream.do_identifying_traits_match(event):
                return stream

        # New stream ...
        stream = Stream(self.identifying_trait_names, event)
        self.active_streams[stream.uuid] = stream
        return stream

    def should_trigger(self, stream, last_event):
        if self.trigger_rule.should_trigger(stream, last_event):
            self.trigger_callback(stream)
            del self.active_streams[stream.uuid]


class Pipeline(object):
    def __init__(self, rules):
        self.rules = rules  # [StreamRule, ...]

    def add_event(self, event):
        message_id = event.get('message_id')
        if not message_id:
            raise BadEvent("Event has no message_id")

        stream = self._find_stream_for_event(message_id, event)
        stream.add_message(message_id)

    def _find_stream_for_event(self, message_id, event):
        for rule in self.rules:
            stream = rule.get_active_stream(event)
            if stream:
                stream.add_message(message_id)
                if rule.should_trigger(stream, event):
                    rule.trigger(stream)

