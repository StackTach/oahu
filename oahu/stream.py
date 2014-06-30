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


class WrongStateException(Exception):
    pass


class Stream(object):
    def __init__(self, identifying_traits, event):
        self.message_ids = []
        self.uuid = str(uuid.uuid4())
        self.last_update = datetime.datetime.utcnow()
        self._extract_identifying_traits(identifying_traits, event)
        self.state = COLLECTING

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

    def __str__(self):
        return "<Stream %s: %d elements - %s>" % (self.uuid,
                                                  len(self.message_ids),
                                                  self.last_update)
    def trigger(self):
        if self.state != COLLECTING:
            raise WrongStateException("Unable to move to %s state from %s" %
                (readable['TRIGGERED'], readable[self.state]))

        self.state = TRIGGERED

    def processed(self):
        if self.state != TRIGGERED:
            raise WrongStateException("Unable to move to %s state from %s" %
                (readable['PROCESSED'], readable[self.state]))

        self.state = PROCESSED
