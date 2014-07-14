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
import datetime


class TriggerRule(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def should_trigger(self, stream, last_event, now=None):
        return False


class Inactive(TriggerRule):
    def __init__(self, expiry_in_seconds):
        super(Inactive, self).__init__()
        self.expiry_in_seconds = expiry_in_seconds

    def should_trigger(self, stream, last_event, now=None):
        secs = (now - stream.last_update).seconds
        #print "Stream %s = %d seconds (%d)" % (stream.uuid, secs, self.expiry_in_seconds)
        if now is None:
            now = datetime.datetime.utcnow()
        return (now - stream.last_update).seconds > self.expiry_in_seconds
