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
import uuid

import notigen

from oahu import inmemory
from oahu import mongodb_driver
from oahu import trigger_definition
from oahu import pipeline_callback
from oahu import criteria
from oahu import pipeline


class OutOfOrderException(Exception):
    pass


class TestCallback(pipeline_callback.PipelineCallback):
    def __init__(self):
        super(TestCallback, self).__init__()
        self.triggered = 0
        self.streams = {}
        self.request_set = set()

    def on_trigger(self, stream):
        self.triggered += 1
        self.streams[stream.uuid] = stream
        self.request_set.add(stream.events[0]['request_id'])

        last = None
        for event in stream.events:
            if last != None:
                if last['when'] > event['when']:
                    raise OutOfOrderException("%s > %s" %
                                            (last['when'], event['when']))
            last = event


class TestPipeline(unittest.TestCase):
    def _pipeline(self, driver, trigger_name, callback):
        p = pipeline.Pipeline(driver)

        driver.flush_all()
        self.assertEqual(0, driver.get_num_active_streams(trigger_name))

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

        self.assertTrue(driver.get_num_active_streams(trigger_name) > 0)
        now += datetime.timedelta(seconds=2)

        # no chunk size specified = all at once.
        p.do_expiry_check(now)
        p.process_ready_streams(now)
        p.purge_streams()
        self.assertEqual(0, driver.get_num_active_streams(trigger_name))
        self.assertEqual(len(unique), callback.triggered)
        self.assertEqual(len(unique), len(callback.streams))
        self.assertEqual(unique, callback.request_set)

    def _get_rules(self):
        inactive = criteria.Inactive(60)
        callback = TestCallback()
        trigger_name = str(uuid.uuid4())
        by_request = trigger_definition.TriggerDefinition(trigger_name,
                                             ["request_id", ],
                                             inactive, callback)
        rules = [by_request, ]

        return (rules, callback, trigger_name)

    # TODO(sandy): The drivers for these tests will come from a configuration
    # and simport'ed.

    def test_inmemory(self):
        rules, callback, trigger_name = self._get_rules()
        driver = inmemory.InMemoryDriver(rules)
        self._pipeline(driver, trigger_name, callback)

    def test_mongo(self):
        rules, callback, trigger_name = self._get_rules()
        driver = mongodb_driver.MongoDBDriver(rules)
        self._pipeline(driver, trigger_name, callback)
