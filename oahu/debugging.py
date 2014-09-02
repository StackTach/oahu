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


class SimpleDumper(object):
    def dump_trait_match(self, debugger):
        print "%s: %d of %d trait match = %d new streams" % (
            debugger._name,
            debugger._trait_match,
            debugger._trait_match+debugger._trait_mismatch,
            debugger._new_streams)

    def dump_criteria_match(self, debugger):
        print "%s: %d of %d criteria match" % (
            debugger._name,
            debugger._criteria_match,
            debugger._criteria_match+debugger._criteria_mismatch)

    def dump_errors(self, debugger):
        print "%s: %d on_trigger() errors, %d commit() errors" % (
            debugger._name,
            debugger._trigger_errors,
            debugger._commit_errors)


class DetailedDumper(SimpleDumper):
    def dump_criteria_match(self, debugger):
        super(DetailedDumper, self).dump_criteria_match(debugger)

        for reason in debugger._reasons.iteritems():
            print " - '%s' mismatches = %d" % reason

        # TODO(sandy): Should add provisions for exception counts.


def dump_debugger(debugger, trait_match, criteria_match, errors):
    if trait_match:
        debugger.dump_trait_match()
    if criteria_match:
        debugger.dump_criteria_match()
    if errors:
        debugger.dump_errors()
    debugger.reset()


class NoOpTriggerDebugger(object):
    def __init__(self, *args, **kwargs):
        pass

    def dump_trait_match(self):
        pass

    def dump_criteria_match(self):
        pass

    def dump_errors(self):
        pass

    def reset(self):
        pass

    def trait_match(self):
        return True

    def trait_mismatch(self):
        return False

    def new_stream(self):
        pass

    def criteria_match(self):
        return True

    def criteria_mismatch(self, reason):
        return False

    def check(self, value, reason):
        return bool(value)

    def trigger_error(self):
        return False

    def commit_error(self):
        return False


class TriggerDebugger(object):
    def __init__(self, name, dumper=None):
        self._name = name
        if dumper:
            self.dumper = dumper
        else:
            self.dumper = SimpleDumper()
        self.reset()

    def dump_trait_match(self):
        self.dumper.dump_trait_match(self)

    def dump_criteria_match(self):
        self.dumper.dump_criteria_match(self)

    def dump_errors(self):
        self.dumper.dump_errors(self)

    def reset(self):
        # If it's not a match or a mismatch it was a fatal error.
        self._trait_mismatch = 0
        self._trait_match = 0
        self._new_streams = 0

        self._criteria_match = 0
        self._criteria_mismatch = 0
        self._reasons = {}

        self._trigger_errors = 0
        self._commit_errors = 0

    def trait_match(self):
        self._trait_match += 1
        return True

    def trait_mismatch(self):
        self._trait_mismatch += 1
        return False

    def new_stream(self):
        self._new_streams += 1

    def criteria_match(self):
        self._criteria_match += 1
        return True

    def criteria_mismatch(self, reason):
        self._criteria_mismatch += 1
        self._reasons[reason] = self._reasons.get(reason, 0) + 1
        return False

    def check(self, value, reason):
        if value:
            return self.criteria_match()
        return self.criteria_mismatch(reason)

    def trigger_error(self):
        self._trigger_errors += 1
        return False

    def commit_error(self):
        self._commit_errors += 1
        return False
