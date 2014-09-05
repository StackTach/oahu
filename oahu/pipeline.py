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

"""There are several places we need to process the streams:
   1. When a new event comes in.
   2. Periodically to check for triggered streams.
   3. Periodically to process ready streams.
   4. Periodically to delete processed streams.
   These last three could be dealing with potentially large sets
   and each operation could be done by multiple workers.
"""


class Pipeline(object):
    def __init__(self, db_driver):
        self.db_driver = db_driver
        self.cursor_state = db_driver.get_cursor_state()

    def add_event(self, event):
        self.db_driver.add_event(event)

    # These methods are called as periodic tasks and
    # may be expensive (in that they may iterate over
    # all streams).
    def do_trigger_check(self, chunk, now=None):
        if now is None:
            now = datetime.datetime.utcnow()
        self.db_driver.do_trigger_check(self.cursor_state, chunk, now)

    def purge_streams(self, chunk):
        self.db_driver.purge_processed_streams(self.cursor_state, chunk)

    def process_ready_streams(self, chunk, now=None):
        """If the stream is ready we need to trigger it and
           process the pipeline.
        """
        if now is None:
            now = datetime.datetime.utcnow()
        self.db_driver.process_ready_streams(self.cursor_state, chunk, now)
