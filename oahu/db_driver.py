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

import abc

import stream as pstream


class BadEvent(Exception):
    pass


class DBDriver(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, trigger_defs):
        self.trigger_defs = trigger_defs  # [TriggerDefinitions, ...]

        # {trigger.name: TriggerDefinition} ... for lookups.
        self.trigger_defs_dict = {}
        for trigger in trigger_defs:
            self.trigger_defs_dict[trigger.name] = trigger

    def add_event(self, event):
        message_id = self._get_message_id(event)
        self.save_event(message_id, event)

        # An event may apply to many streams ...
        for trigger in self.trigger_defs:
            if not trigger.applies(event):
                continue

            trait_dict = trigger.get_identifying_trait_dict(event)
            self.append_event(message_id, trigger, event, trait_dict)

    @abc.abstractmethod
    def save_event(self, message_id, event):
        pass

    @abc.abstractmethod
    def append_event(self, message_id, trigger, event):
        pass

    @abc.abstractmethod
    def do_expiry_check(self, now=None, chunk=-1):
        pass

    @abc.abstractmethod
    def purge_processed_streams(self, chunk=-1):
        pass

    @abc.abstractmethod
    def process_ready_streams(self, now, chunk=-1):
        pass

    @abc.abstractmethod
    def ready(self, trigger_name, stream):
        pass

    @abc.abstractmethod
    def trigger(self, trigger_name, stream):
        pass

    @abc.abstractmethod
    def get_num_active_streams(self, trigger_name):
        pass

    @abc.abstractmethod
    def flush_all(self):
        pass

    def _get_message_id(self, event):
        # We save the event, but only deal with the
        # message_id during stream processing.
        message_id = event.get('message_id')
        if not message_id:
            raise BadEvent("Event has no message_id")

        return message_id

    def _check_for_trigger(self, trigger, stream, event=None, now=None):
        # Duck-typing assumed on the stream object. So long as it
        # has a .state attribute and whatever is needed by the
        # rule object.
        if stream.state != pstream.COLLECTING:
            return False
        if trigger.should_fire(stream, event, now=now):
            self.ready(trigger.name, stream)
            return True
        return False