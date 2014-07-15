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
import uuid

import pymongo

import sync_engine
import stream as pstream


# Collections:
# ["events"] = event docs
#
# ["rules"] = { 'rule_id',
#               'stream_id',
#               'identifying_traits': {trait: value, ...},
#               'last_update',
#               'state',
#             } where xxx = rule_id
#
# ["streams'] = {'stream_id', 'message_id'}
#


class MongoDBSyncEngine(sync_engine.SyncEngine):
    """Trivial Sync Engine that works in a distributed fashion.
       For testing only. Do not attempt to use in production.
    """

    def __init__(self, rules):
        super(MongoDBSyncEngine, self).__init__(rules)
        self.client = pymongo.MongoClient()
        self.db = self.client['stacktach']

        self.events = self.db['events']
        self.events.ensure_index("message_id")
        self.events.ensure_index("when")
        self.events.ensure_index("request_id")

        self.rule_collection = self.db['rules']
        self.rule_collection.ensure_index("rule_id")
        self.rule_collection.ensure_index("stream_id")
        self.rule_collection.ensure_index("state")
        self.rule_collection.ensure_index("last_update")
        self.rule_collection.ensure_index("identifying_traits")

        self.streams = self.db['streams']
        self.streams.ensure_index('stream_id')
        self.streams.ensure_index('when')

    def save_event(self, message_id, event):
        self.events.insert(event)

    def append_event(self, message_id, rule, event, trait_dict):
        # Find the stream (or make one) and tack on the message_id.

        stream_id = None
        for doc in self.rule_collection.find({'rule_id': rule.rule_id,
                                              'state': pstream.COLLECTING,
                                              'identifying_traits': trait_dict}):
            stream_id = doc['stream_id']
            break

        now  = datetime.datetime.utcnow()
        update_time = True
        if not stream_id:
            # Make a new Stream for this trait_dict ...
            stream_id = str(uuid.uuid4())
            stream = {'stream_id': stream_id,
                      'rule_id': rule.rule_id,
                      'last_update': now,
                      'identifying_traits': trait_dict,
                      'state_version': 1,
                      'state': pstream.COLLECTING,
                     }
            update_time = False
            self.rule_collection.insert(stream)

        # Add this message_id to the stream collection ...
        entry = {'stream_id': stream_id,
                 'when': event['when'],
                 'message_id': message_id}
        self.streams.insert(entry)

        if update_time:
            self.rule_collection.update({'stream_id': stream_id},
                                        {'$set': {'last_update': now}})

    def do_expiry_check(self, now=None, chunk=-1):
        # TODO(sandy) - we need to get the expiry time as part of the
        #               stream document so the search is optimal.
        num = 0
        ready = 0

        query = self.rule_collection.find({'state': pstream.COLLECTING}).sort(
                                [('last_update', pymongo.ASCENDING)])
        if chunk > 0:
            query = query.limit(chunk)
        for doc in query:
            rule_id = doc['rule_id']
            rule = self.rules_dict[rule_id]
            num += 1

            stream = pstream.Stream(doc['stream_id'],
                                    rule_id,
                                    doc['state'],
                                    doc['last_update'])
            if self._check_for_trigger(rule, stream, now=now):
                ready += 1
        print "%s - checked %d (%d ready)" % (now, num, ready)

    def purge_processed_streams(self, chunk=-1):
        now = datetime.datetime.utcnow()
        print "%s - purged %d" % (now,
            self.rule_collection.remove({'state': pstream.PROCESSED})['n'])

    def process_ready_streams(self, now, chunk=-1):
        num = 0
        locked = 0
        query = self.rule_collection.find({'state': pstream.READY})
        if chunk > 0:
            query = query.limit(chunk)
        for ready in query:
            result = self.rule_collection.update({'_id': ready['_id'],
                                                  'state_version': ready['state_version']},
                                                 {'$set': {'state': pstream.TRIGGERED},
                                                  '$inc': {'state_version': 1}},
                                                  safe=True)
            if result['n'] == 0:
                locked += 1
                continue  # Someone else got it first, move to next one.

            stream_id = ready['stream_id']
            stream = pstream.Stream(stream_id,
                                    ready['rule_id'],
                                    ready['state'],
                                    ready['last_update'])

            num += 1
            events = []
            for mdoc in self.streams.find({'stream_id': stream_id}) \
                                    .sort('when', pymongo.ASCENDING):
                for e in self.events.find({'message_id': mdoc['message_id']}):
                    events.append(e)

            stream.set_events(events)
            rule = self.rules_dict[ready['rule_id']]
            rule.trigger_callback.on_trigger(stream)
            self.rule_collection.update({'stream_id': stream_id},
                                    {'$set': {'state': pstream.PROCESSED}})
        print "%s - processed %d/%d (%d locked)" % (now, num, chunk, locked)

    def trigger(self, rule_id, stream):
        self.rule_collection.update({'stream_id': stream.uuid},
                                    {'$set': {'state': pstream.TRIGGERED}})

    def ready(self, rule_id, stream):
        self.rule_collection.update({'stream_id': stream.uuid},
                                    {'$set': {'state': pstream.READY}})

    def get_num_active_streams(self, rule_id):
        return self.rule_collection.find({'rule_id': rule_id}).count()

    def flush_all(self):
        self.db.drop_collection('rules')
        self.db.drop_collection('streams')
        self.db.drop_collection('events')
