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
import dateutil.parser

import yagi.config
import yagi.handler
import yagi.log
import yagi.utils

import oahu.config
from oahu import mongodb_driver as driver
from oahu import pipeline
from oahu import pipeline_callback


LOG = yagi.log.logger


class OahuHandler(yagi.handler.BaseHandler):
    """Write the event to the Oaha pipeline.
    """
    AUTO_ACK = True

    def __init__(self, app=None, queue_name=None):
        super(OahuHandler, self).__init__(app, queue_name)
        # Don't use interpolation from ConfigParser ...
        self.config = dict(yagi.config.config.items('oahu', raw=True))

        config_simport_location = self.config['config_class']
        self.oahu_config = oahu.config.get_config(config_simport_location)
        self.driver = self.oahu_config.get_driver()
        self.pipeline = pipeline.Pipeline(self.driver)

        # TODO(sandy) - wipe the database every time, for now.
        self.driver.flush_all()

        self.last = datetime.datetime.utcnow()
        self.processed = 0

    def handle_messages(self, messages, env):
        for event in self.iterate_payloads(messages, env):
            # TODO(sandy) - we will need to run the raw event
            # through the distiller and use the reduced set of Traits.
            # But, until we do, we're going to massage the full notification
            # to get what we need.

            payload = event['payload']

            when = dateutil.parser.parse(event['timestamp'])
            audit = when
            a_beginning = payload.get('audit_period_beginning')
            if a_beginning:
                audit = dateutil.parser.parse(a_beginning)
            event['audit_bucket'] = str(audit.date())
            event['timestamp'] = when  # force to datetime

            try:
                self.pipeline.add_event(event)
            except Exception as ex:
                print ex

            self.processed += 1

        now = datetime.datetime.utcnow()

        if (now - self.last).seconds > 10:
            self.last = now
            print "Added %d events at %s" % (self.processed, now)
            self.processed = 0
            self.driver.dump_debuggers(criteria_match=False, errors=False)
