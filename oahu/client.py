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
  pipeline errors <config_simport> [--commit_only|--trigger_only]
  pipeline stream <config_simport> <stream_id>
  pipeline (-h | --help)
  pipeline --version

Options:
  -h --help              Show this help message
  --version              Show pipeline version
  --debug                Debug mode
  --daemon               Run as daemon
  --polling_rate=<rate>  Rate in seconds [default: 300]
  <config_simport>       Config class location in Simport format
  <stream_id>            ID of stream
  --commit_only          show only commit-stage errors
  --trigger_only         show only trigger-stage errors

"""
import datetime
import time

import daemon
from docopt import docopt
import prettytable

from oahu import config
from oahu import mongodb_driver as driver
from oahu import pipeline
from oahu import stream


def run(poll, expired, ready, completed, conf):
    print "Polling rate:", poll

    db_driver = conf.get_driver()
    p = pipeline.Pipeline(db_driver)

    while True:
        now = datetime.datetime.utcnow()
        if expired:
            p.do_expiry_check(conf.get_expiry_chunk_size(), now)
            db_driver.dump_debuggers(trait_match=False, errors=False)
        if ready:
            p.process_ready_streams(conf.get_ready_chunk_size(), now)
            db_driver.dump_debuggers(criteria_match=False, trait_match=False)
        if completed:
            p.purge_streams(conf.get_completed_chunk_size())

        time.sleep(poll)


def list_errors(conf, trigger_only, commit_only):
    # python git/oahu/oahu/client.py errors ".|oahu_config:Config"
    db_driver = conf.get_driver()
    states = [stream.ERROR, stream.COMMIT_ERROR]
    if trigger_only:
        states = [stream.ERROR,]
    if commit_only:
        states = [stream.COMMIT_ERROR,]
    for state in states:
        print "'%s' State Streams:" % stream.readable[state]
        tdef = db_driver.get_streams_by_state(state, 1000, 0)
        table = prettytable.PrettyTable(["ID", "Trigger", "Last Error"])
        for t in tdef:
            table.add_row([t['stream_id'], t['trigger_name'], t['last_error']])
        print str(table)


def main():
    arguments = docopt(__doc__)

    driver_location = arguments['<config_simport>']
    conf = config.get_config(driver_location)

    if arguments["errors"]:
        list_errors(conf, arguments['--trigger_only'],
                    arguments['--commit_only'])
        return

    expired = arguments["expired"]
    ready = arguments["ready"]
    completed = arguments["completed"]
    poll = float(arguments['--polling_rate'])

    if arguments['--daemon']:
        with daemon.DaemonContext():
            run(poll, expired, ready, completed, conf)
    else:
        run(poll, expired, ready, completed, conf)


if __name__ == '__main__':
    main()
