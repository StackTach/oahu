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


class TriggerDefinition(object):
    def __init__(self, name, identifying_trait_names, criteria,
                 pipeline_callback):
        self.name = name
        self.identifying_trait_names = identifying_trait_names
        self.criteria = criteria
        self.pipeline_callback = pipeline_callback

    def applies(self, event):
        """Returns True if the trait names apply to the supplied Event.

        The default behavior says: if you have the identifying traits
        then this trigger def applies. Override for more complex stuff.
        """
        for name in self.identifying_trait_names:
            if name not in event:
                return False
        return True

    def get_identifying_trait_names(self):
        return self.identifying_trait_names

    def get_identifying_trait_dict(self, event):
        return dict((trait, event[trait]) for trait in
                                        self.identifying_trait_names)

    def should_fire(self, stream, last_event, now=None):
        """last_event could be None if we're doing a periodic check.
        """
        return self.criteria.should_fire(stream, last_event, now=now)
