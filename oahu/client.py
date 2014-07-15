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
  pipeline (expired|ready|completed) <config_simport> [--daemon] [--polling_rate=<rate>]
  pipeline (-h | --help)
  pipeline --version

Options:
  -h --help              Show this help message
  --version              Show pipeline version
  --debug                Debug mode
  --daemon               Run as daemon
  --polling_rate=<rate>  Rate in seconds [default: 300]
  <config_simport>       Config class location in Simport format

"""
import datetime
import time

import daemon
from docopt import docopt

from oahu import config
from oahu import mongodb_sync_engine as driver
from oahu import pipeline
from oahu import stream_rules
from oahu import trigger_callback
from oahu import trigger_rule


def run(poll, expired, ready, completed, conf):
    print "Polling rate:", poll

    sync_engine = conf.get_sync_engine()
    p = pipeline.Pipeline(sync_engine)

    while True:
        now = datetime.datetime.utcnow()
        if expired:
            p.do_expiry_check(now, chunk=conf.get_expiry_chunk_size())
        if ready:
            p.process_ready_streams(now, chunk=conf.get_ready_chunk_size())
        if completed:
            p.purge_streams(chunk=conf.get_completed_chunk_size())

        time.sleep(poll)


def main():
    arguments = docopt(__doc__)

    expired = arguments["expired"]
    ready = arguments["ready"]
    completed = arguments["completed"]
    poll = float(arguments['--polling_rate'])

    sync_engine_location = arguments['<config_simport>']
    conf = config.get_config(sync_engine_location)

    if arguments['--daemon']:
        with daemon.DaemonContext():
            run(poll, expired, ready, completed, conf)
    else:
        run(poll, expired, ready, completed, conf)


if __name__ == '__main__':
    main()
