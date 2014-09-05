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
  pipeline (trigger|ready|completed) <config_simport> [--daemon] [--polling_rate=<rate>]
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
from oahu import mongodb_driver as driver
from oahu import pipeline
from oahu import stream


def run(poll, trigger, ready, completed, conf):
    print "Polling rate:", poll

    db_driver = conf.get_driver()
    p = pipeline.Pipeline(db_driver)

    while True:
        now = datetime.datetime.utcnow()
        if trigger:
            p.do_trigger_check(conf.get_trigger_chunk_size(), now)
            db_driver.dump_debuggers(trait_match=False, errors=False)
        if ready:
            p.process_ready_streams(conf.get_ready_chunk_size(), now)
            db_driver.dump_debuggers(criteria_match=False, trait_match=False)
        if completed:
            p.purge_streams(conf.get_completed_chunk_size())

        time.sleep(poll)


def main():
    arguments = docopt(__doc__)

    driver_location = arguments['<config_simport>']
    conf = config.get_config(driver_location)

    trigger = arguments["trigger"]
    ready = arguments["ready"]
    completed = arguments["completed"]
    poll = float(arguments['--polling_rate'])

    if arguments['--daemon']:
        with daemon.DaemonContext():
            run(poll, trigger, ready, completed, conf)
    else:
        run(poll, trigger, ready, completed, conf)


if __name__ == '__main__':
    main()
