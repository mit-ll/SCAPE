# Copyright (2016) Massachusetts Institute of Technology.  Reproduction/Use 
# of all or any part of this material shall acknowledge the MIT Lincoln 
# Laboratory as the source under the sponsorship of the US Air Force 
# Contract No. FA8721-05-C-0002.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''Wrapper for Acqua SelectFactory

'''
import collections

import acqua

import scape_acqua.utils as utils
import scape_acqua.operators as operators

class SelectFactory(object):
    ''' Acqua SelectFactory wrapper

    >>>
    >>>
    '''
    def __init__(self, connection):
        self.factory = acqua.SelectFactory.getInstance(
            connection.configuration, connection.database,
        )
        self.connection = connection

    def select(self, table, start, stop, where=None, fields=None):
        field_list = utils.string_list(fields)

        start_ed = utils.event_date(start)
        stop_ed = utils.event_date(stop)

        return operators.AcquaSelect.from_factory(
            self.factory.createSelect(
                field_list, table, start_ed, stop_ed, where,
            ), self,
        )

