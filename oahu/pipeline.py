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
   2. Periodically to check for expired streams.
   3. Periodically to process triggered streams.
   4. Periodically to delete processed streams.
   These last three could be dealing with potentially large sets
   and each operation could be done by multiple workers.
"""


class Pipeline(object):
    def __init__(self, sync_engine):
        self.sync_engine = sync_engine

    def add_event(self, event):
        self.sync_engine.add_event(event)

    # These methods are called as periodic tasks and
    # may be expensive (in that they may iterate over
    # all streams).
    def do_expiry_check(self, now=None):
        if now is None:
            now = datetime.datetime.utcnow()
        self.sync_engine.do_expiry_check(now)

    def purge_streams(self):
        self.sync_engine.purge_processed_streams()

    def process_triggered_streams(self, now=None):
        """If the stream is triggered we need to process the
           pipeline.
        """
        if now is None:
            now = datetime.datetime.utcnow()
        self.sync_engine.process_triggered_streams(now)
