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
import mock
import unittest

import notigen

from oahu import stream_rules
from oahu import trigger_callback
from oahu import trigger_rule
from oahu import pipeline


class TestCallback(object):
    def on_trigger(self, stream):
        print "Trigger: ", stream

    def on_expiry(self, stream):
        print "Expiry: ", stream


class TestPipeline(unittest.TestCase):
    def test_pipeline(self):
        one_minute = trigger_rule.Inactive(60)
        by_request = stream_rules.StreamRule(["request_id", ],
                                             one_minute,
                                             TestCallback())
        rules = [by_request, ]
        p = pipeline.Pipeline(rules)

        g = notigen.EventGenerator(100)
        now = datetime.datetime.utcnow()
        start = now
        nevents = 0
        while nevents < 10000:
            events = g.generate(now)
            if events:
                for event in events:
                    p.add_event(event)
                nevents += len(events)
                now = datetime.datetime.utcnow()
