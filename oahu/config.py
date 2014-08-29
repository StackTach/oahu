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

import abc

import simport


class Config(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_driver(self):
        pass

    @abc.abstractmethod
    def get_distiller_config(self):
        pass

    def get_ready_chunk_size(self):
        return -1

    def get_expiry_chunk_size(self):
        return -1

    def get_completed_chunk_size(self):
        return -1


def get_config(driver_location):
    config_class = simport.load(driver_location)
    return config_class()
