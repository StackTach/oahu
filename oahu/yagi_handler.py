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

import yagi.config
import yagi.handler
import yagi.log
import yagi.utils

from oahu import mongodb_sync_engine as driver
from oahu import pipeline
from oahu import stream_rules
from oahu import trigger_callback
from oahu import trigger_rule


class Callback(object):
    def on_trigger(self, stream):
        print "Got: ", stream
        for event in stream.events:
            print event['event_type'], event['when']


LOG = yagi.log.logger


class OahuHandler(yagi.handler.BaseHandler):
    """Write the event to the Oaha pipeline.
    """

    def __init__(self, app=None, queue_name=None):
        super(OahuHandler, self).__init__(app, queue_name)
        # Don't use interpolation from ConfigParser ...
        self.config = dict(yagi.config.config.items('oahu', raw=True))

        inactive = trigger_rule.Inactive(60)
        self.callback = Callback()
        rule_id = "request-id"  # Has to be consistent across yagi workers.
        by_request = stream_rules.StreamRule(rule_id,
                                             ["request_id", ],
                                             inactive, self.callback)
        self.rules = [by_request, ]

        self.sync_engine = driver.MongoDBSyncEngine(self.rules)
        self.pipeline = pipeline.Pipeline(self.sync_engine)

        # TODO(sandy) - wipe the database everytime for now
        self.sync_engine.flush_all()

        self.last = datetime.datetime.utcnow()
        self.processed = 0

    def handle_messages(self, messages, env):
        for payload in self.iterate_payloads(messages, env):
            self.pipeline.add_event(payload)
            self.processed += 1

        now = datetime.datetime.utcnow()
        if (now - self.last).seconds > 10:
            self.last = now
            print "Added %d events at %s" % (self.processed, now)
            self.processed = 0
