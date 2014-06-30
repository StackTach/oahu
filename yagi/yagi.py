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

import notification_utils
import yagi.config
import yagi.handler
import yagi.log
import yagi.utils

from oahu import pipeline


LOG = yagi.log.logger


class OahaHandler(yagi.handler.BaseHandler):
    """Write the event to the Oaha pipeline.
    """

    def __init__(self, app=None, queue_name=None):
        super(OahaHandler, self).__init__(app, queue_name)
        # Don't use interpolation from ConfigParser ...
        self.config = dict(yagi.config.config.items('oaha', raw=True))
        self.rules = []
        self.pipeline = pipeline.Pipeline(rules)

    def handle_messages(self, messages, env):
        for payload in self.iterate_payloads(messages, env):
            self.pipeline.add_event(payload)
