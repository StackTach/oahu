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
                 pipeline_callbacks, debug=False, dumper=None):
        self.name = name
        self.identifying_trait_names = identifying_trait_names
        self.criteria = criteria
        self.pipeline_callbacks = pipeline_callbacks
        self.debug = debug  # True/False, debug this TriggerDef?
        self.dumper = dumper  # Which debugging dumper to use if True?

    def __str__(self):
        return "<TriggerDef %s>" % self.name

    def applies(self, event):
        """Returns True if the trait names apply to the supplied Event.

        The default behavior says: if you have the identifying traits
        then this trigger def applies. Override for more complex stuff.

        identifying trait names can be / separated to decend the nested
        event map. For example "payload/instance_id" will get
        {"payload": {"instance_id": "123"}}

        Returns True if the path exists, False otherwise.
        We don't care about the value, that's someone else's job.
        """
        for path in self.identifying_trait_names:
            try:
                value = self._fetch(path, event)
            except KeyError:
                return False
        return True

    def get_identifying_trait_names(self):
        return self.identifying_trait_names

    def get_identifying_trait_dict(self, event):
        """Will skip any missing key values. But this
           shouldn't be an issue since we should never
           reach this call if applies() returns False.

           We wouldn't want to assume None as the value
           since that could be meaningful.
           """
        result = {}
        for path in self.identifying_trait_names:
            try:
                result[path] = self._fetch(path, event)
            except KeyError:
                pass
        return result

    def _fetch(self, path, event):
        parts = path.split('/')
        for name in parts[:-1]:
            event = event[name]
        return event[parts[-1]]

    def should_fire(self, stream, last_event, debugger, now=None):
        """last_event could be None if we're doing a periodic check.
        """
        return self.criteria.should_fire(stream, last_event, debugger,
                                         now=now)
