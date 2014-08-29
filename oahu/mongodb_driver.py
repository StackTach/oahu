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

import copy
import datetime
import json
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
#                      'commit_errors',
#                      'last_error',
#                      'state',
#                    }
#
# ["streams'] = {'stream_id', 'message_id'}


class Stream(pstream.Stream):
    def __init__(self, uuid, trigger_name, state, last_update,
                 identifying_traits, driver):
        super(Stream, self).__init__(uuid, trigger_name, state, last_update,
                                     identifying_traits)
        self.driver = driver
        self.events_loaded = False

    def load_events(self):
        if self.events_loaded:
            return
        self.driver._load_events(self)
        self.events_loaded = True

    def error(self, last_exception):
        self.driver.error(self, last_exception)

    def commit_error(self, last_exception):
        self.driver.commit_error(self, last_exception)


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
        self.events.ensure_index("_context_request_id")

        self.tdef_collection = self.db['trigger_defs']
        self.tdef_collection.ensure_index("trigger_name")
        self.tdef_collection.ensure_index("stream_id")
        self.tdef_collection.ensure_index("state")
        self.tdef_collection.ensure_index("last_update")
        self.tdef_collection.ensure_index("identifying_traits")

        self.streams = self.db['streams']
        self.streams.ensure_index('stream_id')
        self.streams.ensure_index('when')

    def _scrub_event(self, event):
        if type(event) is list:
            for x in event:
                self._scrub_event(x)
        elif type(event) is dict:
            to_delete = []
            to_add = []
            for k, v in event.iteritems():
                if '.' in k:
                    new_k = k.replace('.', '~')
                    to_delete.append(k)
                    to_add.append((new_k, v))
                self._scrub_event(v)
            for k in to_delete:
                del event[k]
            for k, v in to_add:
                event[k] = v

    def save_event(self, message_id, event):
        safe = copy.deepcopy(event)
        self._scrub_event(safe)
        safe['message_id'] = message_id  # Force to known location.
        self.events.insert(safe)

    def append_event(self, message_id, trigger_def, event, trait_dict):
        # Find the stream (or make one) and tack on the message_id.

        stream_id = None
        for doc in self.tdef_collection.find({'trigger_name': trigger_def.name,
                                              'state': pstream.COLLECTING,
                                              'identifying_traits': trait_dict}
                                            ).limit(1):
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
                      'commit_errors': 0,
                      'last_error': "",
                      'state': pstream.COLLECTING,
                     }
            update_time = False
            self.tdef_collection.insert(stream)

        # Add this message_id to the stream collection ...
        entry = {'stream_id': stream_id,
                 'when': event['timestamp'],
                 'message_id': message_id}
        self.streams.insert(entry)

        if update_time:
            self.tdef_collection.update({'stream_id': stream_id},
                                        {'$set': {'last_update': now}})
        return not update_time  # a new stream if we didn't update the time.

    def do_expiry_check(self, state, chunk, now=None):
        # TODO(sandy) - we need to get the expiry time as part of the
        #               stream document so the search is optimal.
        num = 0
        ready = 0

        query = self.tdef_collection.find({'state': pstream.COLLECTING}).sort(
                                [('last_update', pymongo.ASCENDING)]
                            ).skip(state.offset).limit(chunk)
        for doc in query:
            trigger_name = doc['trigger_name']
            trigger = self.trigger_defs_dict[trigger_name]
            num += 1

            stream = Stream(doc['stream_id'], trigger_name, doc['state'],
                            doc['last_update'], doc['identifying_traits'],
                            self)
            if self._check_for_trigger(trigger, stream, now=now):
                ready += 1

        size = query.retrieved
        print "%s - checked %d (%d ready) off/lim/sz=%d/%d/%d" % (
                                                    now, num, ready,
                                                    state.offset, chunk, size)
        if size < chunk:
            state.offset = 0
        else:
            state.offset += num

    def purge_processed_streams(self, state, chunk):
        now = datetime.datetime.utcnow()
        print "%s - purged %d" % (now,
            self.tdef_collection.remove({'state': pstream.PROCESSED})['n'])

    def _load_events(self, stream):
        events = []
        hit = False
        x = self.streams.find({'stream_id': stream.uuid}) \
                             .sort('when', pymongo.ASCENDING)
        #print "Stream: %s" % stream.uuid
        for mdoc in x:
            for e in self.events.find({'message_id': mdoc['message_id']}):
                events.append(e)
                #print e['event_type'], e['payload'].get(
                #        'audit_period_beginning',
                #        "nothinghere")[-8:] == "00:00:00", e['timestamp']
        stream.set_events(events)

    def process_ready_streams(self, state, chunk, now):
        num = 0
        locked = 0
        query = self.tdef_collection.find({'state': pstream.READY}
                                         ).limit(chunk).skip(state.offset)
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
            stream = Stream(stream_id, ready['trigger_name'], ready['state'],
                            ready['last_update'], ready['identifying_traits'],
                            self)

            num += 1
            self._load_events(stream)
            trigger = self.trigger_defs_dict[ready['trigger_name']]
            self._do_pipeline_callbacks(stream, trigger)
        size = query.retrieved
        if size < chunk:
            state.offset = 0
        else:
            state.offset += num

        print "%s - processed %d/%d (%d locked, off/lim/sz: %d/%d/%d)" % (
                                                now, num,
                                                chunk, locked,
                                                state.offset, chunk, size)

    def trigger(self, trigger_name, stream):
        self.tdef_collection.update({'stream_id': stream.uuid},
                                 {'$set': {'state': pstream.TRIGGERED}})

    def ready(self, trigger_name, stream):
        self.tdef_collection.update({'stream_id': stream.uuid},
                                 {'$set': {'state': pstream.READY}})

    def processed(self, trigger_name, stream):
        self.tdef_collection.update({'stream_id': stream.uuid},
                                 {'$set': {'state': pstream.PROCESSED}})

    def error(self, trigger_name, stream, error):
        self.tdef_collection.update({'stream_id': stream.uuid},
                                 {'$set': {'state': pstream.ERROR,
                                           'last_error': error}})

    def commit_error(self, trigger_name, stream, error):
        self.tdef_collection.update({'stream_id': stream.uuid},
                                 {'$set': {'state': pstream.COMMIT_ERROR,
                                           'last_error': error},
                                  '$inc': {'commit_errors': 1},
                                 })

    def get_num_active_streams(self, trigger_name):
        return self.tdef_collection.find({'trigger_name': trigger_name}
                                        ).count()

    def flush_all(self):
        self.db.drop_collection('trigger_defs')
        self.db.drop_collection('streams')
        self.db.drop_collection('events')
