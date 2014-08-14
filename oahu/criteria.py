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


class Criteria(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def should_fire(self, stream, last_event, now=None):
        return False


class Inactive(Criteria):
    def __init__(self, expiry_in_seconds):
        super(Inactive, self).__init__()
        self.expiry_in_seconds = expiry_in_seconds

    def should_fire(self, stream, last_event, now=None):
        secs = (now - stream.last_update).seconds
        #print "Stream %s = %d seconds (%d)" % (stream.uuid, secs, self.expiry_in_seconds)
        if now is None:
            now = datetime.datetime.utcnow()
        return (now - stream.last_update).seconds > self.expiry_in_seconds


class EventType(Criteria):
    def __init__(self, event_type):
        super(EventType, self).__init__()
        self.event_type = event_type

    def should_fire(self, stream, last_event, now=None):
        if not last_event:
            return False
        return last_event['event_type'] == self.event_type


class And(Criteria):
    def __init__(self, criteria_list):
        super(And, self).__init__()
        self.criteria_list = criteria_list

    def should_fire(self, stream, last_event, now=None):
        should = [c.should_fire(stream, last_event, now)
                                        for c in self.criteria_list]
        return all(should)


class EndOfDayExists(Criteria):
    def __init__(self, exists_name):
        super(EndOfDayExists, self).__init__()
        self.exists_name = exists_name

    def _is_zero_hour(self, tyme):
        return tyme.second == 0 and tyme.minute == 0 and tyme.hour == 0

    def should_fire(self, stream, last_event, now=None):
        if not last_event:
            return False

        if last_event['event_type'] != self.exists_name:
            return False

        last_time = last_event.get('when')

        return self._is_zero_hour(last_time)
