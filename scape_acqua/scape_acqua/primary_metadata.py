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

import acqua
import datetime

class PrimaryMetadata(object):
    ''' Wrapper object for Acqua PrimaryMetadata
    '''
    def __init__(self,connection):
        self.metadata = acqua.PrimaryMetadata(
            connection.configuration, connection.database
        )

    def last_time(self, table_meta):
        '''Given table metadata, return the latest timestamp ingested into the
        table. Retrieves this information from the rowID for each
        shard.

        '''
        try:
            date = self.metadata.lastTime(table_meta.metadata)
        except:
            date = None
        if date is not None:
            date = datetime.datetime.fromtimestamp(date.getTime()/1000)
        return date
        
