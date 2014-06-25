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
    triggered = 0

    def on_trigger(self, stream):
        self.triggered += 1


class TestPipeline(unittest.TestCase):
    def test_pipeline(self):
        inactive = trigger_rule.Inactive(60)
        callback = TestCallback()
        by_request = stream_rules.StreamRule(["request_id", ],
                                             inactive, callback)
        rules = [by_request, ]
        p = pipeline.Pipeline(rules)

        g = notigen.EventGenerator(100)
        now = datetime.datetime.utcnow()
        nevents = 0
        unique = set()
        while nevents < 5000:
            events = g.generate(now)
            if events:
                for event in events:
                    p.add_event(event)
                    unique.add(event['request_id'])
                nevents += len(events)
            now = g.move_to_next_tick(now)

        self.assertTrue(len(p.rules[0].active_streams) > 0)
        now += datetime.timedelta(seconds=2)
        p.do_expiry_check(now)
        self.assertEqual(0, len(p.rules[0].active_streams))
        self.assertEqual(len(unique), callback.triggered)
        # TODO(sandy): match unique request_ids
