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

import db_driver
import stream as pstream


# Collections:
# ["events"] = event docs
#
# ["trigger_defs"] = { 'trigger_name',
#                      'stream_id',
#                      'identifying_traits': {trait: value, ...},
#                      'last_update',
#                      'state',
#                    }
#
# ["streams'] = {'stream_id', 'message_id'}
#


class MongoDBDriver(db_driver.DBDriver):
    """Trivial DBDriver that works in a distributed fashion.
       For testing only. Do not attempt to use in production.
    """

    def __init__(self, trigger_defs):
        super(MongoDBDriver, self).__init__(trigger_defs)
        self.client = pymongo.MongoClient()
        self.db = self.client['stacktach']

        self.events = self.db['events']
        self.events.ensure_index("message_id")
        self.events.ensure_index("when")
        self.events.ensure_index("request_id")

        self.tdef_collection = self.db['trigger_defs']
        self.tdef_collection.ensure_index("trigger_name")
        self.tdef_collection.ensure_index("stream_id")
        self.tdef_collection.ensure_index("state")
        self.tdef_collection.ensure_index("last_update")
        self.tdef_collection.ensure_index("identifying_traits")

        self.streams = self.db['streams']
        self.streams.ensure_index('stream_id')
        self.streams.ensure_index('when')

    def save_event(self, message_id, event):
        self.events.insert(event)

    def append_event(self, message_id, trigger_def, event, trait_dict):
        # Find the stream (or make one) and tack on the message_id.

        stream_id = None
        for doc in self.tdef_collection.find({'trigger_name': trigger_def.name,
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
                      'trigger_name': trigger_def.name,
                      'last_update': now,
                      'identifying_traits': trait_dict,
                      'state_version': 1,
                      'state': pstream.COLLECTING,
                     }
            update_time = False
            self.tdef_collection.insert(stream)

        # Add this message_id to the stream collection ...
        entry = {'stream_id': stream_id,
                 'when': event['when'],
                 'message_id': message_id}
        self.streams.insert(entry)

        if update_time:
            self.tdef_collection.update({'stream_id': stream_id},
                                        {'$set': {'last_update': now}})

    def do_expiry_check(self, now=None, chunk=-1):
        # TODO(sandy) - we need to get the expiry time as part of the
        #               stream document so the search is optimal.
        num = 0
        ready = 0

        query = self.tdef_collection.find({'state': pstream.COLLECTING}).sort(
                                [('last_update', pymongo.ASCENDING)])
        if chunk > 0:
            query = query.limit(chunk)
        for doc in query:
            trigger_name = doc['trigger_name']
            trigger = self.trigger_defs_dict[trigger_name]
            num += 1

            stream = pstream.Stream(doc['stream_id'],
                                    trigger_name,
                                    doc['state'],
                                    doc['last_update'])
            if self._check_for_trigger(trigger, stream, now=now):
                ready += 1
        print "%s - checked %d (%d ready)" % (now, num, ready)

    def purge_processed_streams(self, chunk=-1):
        now = datetime.datetime.utcnow()
        print "%s - purged %d" % (now,
            self.tdef_collection.remove({'state': pstream.PROCESSED})['n'])

    def process_ready_streams(self, now, chunk=-1):
        num = 0
        locked = 0
        query = self.tdef_collection.find({'state': pstream.READY})
        if chunk > 0:
            query = query.limit(chunk)
        for ready in query:
            result = self.tdef_collection.update(
                {'_id': ready['_id'],
                 'state_version': ready['state_version']},
                {'$set': {'state': pstream.TRIGGERED},
                 '$inc': {'state_version': 1}},
                 safe=True)
            if result['n'] == 0:
                locked += 1
                continue  # Someone else got it first, move to next one.

            stream_id = ready['stream_id']
            stream = pstream.Stream(stream_id,
                                    ready['trigger_name'],
                                    ready['state'],
                                    ready['last_update'])

            num += 1
            events = []
            for mdoc in self.streams.find({'stream_id': stream_id}) \
                                    .sort('when', pymongo.ASCENDING):
                for e in self.events.find({'message_id': mdoc['message_id']}):
                    events.append(e)

            stream.set_events(events)
            trigger = self.trigger_defs_dict[ready['trigger_name']]
            trigger.pipeline_callback.on_trigger(stream)
            self.tdef_collection.update({'stream_id': stream_id},
                                     {'$set': {'state': pstream.PROCESSED}})
        print "%s - processed %d/%d (%d locked)" % (now, num, chunk, locked)

    def trigger(self, trigger_name, stream):
        self.tdef_collection.update({'stream_id': stream.uuid},
                                 {'$set': {'state': pstream.TRIGGERED}})

    def ready(self, trigger_name, stream):
        self.tdef_collection.update({'stream_id': stream.uuid},
                                 {'$set': {'state': pstream.READY}})

    def get_num_active_streams(self, trigger_name):
        return self.tdef_collection.find({'trigger_name': trigger_name}).count()

    def flush_all(self):
        self.db.drop_collection('trigger_defs')
        self.db.drop_collection('streams')
        self.db.drop_collection('events')
