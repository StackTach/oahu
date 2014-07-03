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


class StreamRule(object):
    def __init__(self, rule_id, identifying_trait_names, trigger_rule,
                 trigger_callback):
        self.rule_id = rule_id
        self.identifying_trait_names = identifying_trait_names
        self.trigger_rule = trigger_rule
        self.trigger_callback = trigger_callback

    def applies(self, event):
        """Returns True if this rule applies to the supplied Event.

        The default behavior says: if you have the identifying traits
        then this rule applies. Override for more complex stuff.
        """
        for name in self.identifying_trait_names:
            if name not in event:
                return False
        return True

    def get_identifying_trait_names(self):
        return self.identifying_trait_names

    def should_trigger(self, stream, last_event, now=None):
        """last_event could be None if we're doing a periodic check.
        """
        return self.trigger_rule.should_trigger(stream, last_event, now=now)
