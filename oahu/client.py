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

"""Pipeline - periodic pipeline processing for StackTach.v3

Usage:
  pipeline (expired|ready|completed) [--daemon] [--polling_rate=<rate>]
  pipeline (-h | --help)
  pipeline --version

Options:
  -h --help              Show this help message
  --version              Show pipeline version
  --debug                Debug mode
  --daemon               Run as daemon
  --polling_rate=<rate>  Rate in seconds [default: 300]

"""
import datetime
import time

import daemon
from docopt import docopt

from oahu import mongodb_sync_engine as driver
from oahu import pipeline
from oahu import stream_rules
from oahu import trigger_callback
from oahu import trigger_rule


class Callback(object):
    def on_trigger(self, stream):
        print "Processing", stream


def run(poll, expired, ready, completed):
    print "Polling rate:", poll

    # TODO(sandy) - This is a copy-paste from the yagi handler.
    # Really need the config file parser for these rules/pipelines, etc.
    inactive = trigger_rule.Inactive(60)
    rule_id = "request-id"  # Has to be consistent across yagi workers.
    callback = Callback()
    by_request = stream_rules.StreamRule(rule_id,
                                         ["request_id", ],
                                         inactive, callback)
    rules = [by_request, ]

    sync_engine = driver.MongoDBSyncEngine(rules)
    p = pipeline.Pipeline(sync_engine)

    while True:
        now = datetime.datetime.utcnow()
        if expired:
            p.do_expiry_check(now)
        if ready:
            p.process_ready_streams(now)
        if completed:
            p.purge_streams()

        time.sleep(poll)


def main():
    arguments = docopt(__doc__)

    expired = arguments["expired"]
    ready = arguments["ready"]
    completed = arguments["completed"]
    poll = float(arguments['--polling_rate'])

    if arguments['--daemon']:
        with daemon.DaemonContext():
            run(poll, expired, ready, completed)
    else:
        run(poll, expired, ready, completed)


if __name__ == '__main__':
    main()
