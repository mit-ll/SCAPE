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

import scape_acqua.operators as operators
import scape_acqua.utils as utils

class Ingestor(object):
    closed = False
    def __init__(self, connection, table_metadata):
        self.ingestor = acqua.Ingestor.getInstance(
            connection.configuration, connection.database
        )
        self.table_metadata = table_metadata.metadata

    def __enter__(self):
        return self
    def __exit__(self,*a,**kw):
        self.close()

    def close(self):
        self.ingestor.close()

    def ingest_row(self,row):
        '''Ingest a row (dictionary)

        If row is a nested dict ({"fam":{"qual":"val"}}), flatten to
        {"fam:qual":"val"}

        '''
        row_map = utils.dict_to_row(row)
        self.ingestor.ingestRow(self.table_metadata, row)

    def ingest_operator(self,operator):
        '''Ingest from an Acqua Operator

        >>> C = acqua.connection.Connection()
        >>> S = acqua.operators.CSVScanner(['file.csv'])
        >>> with C.ingestor('table','time',['colA','colB']) as I:
        ...     I.ingest_operator(S)
        ...
        >>>
        '''
        if isinstance(operator,operators.AcquaOperatorSource):
            operator = operator.operator
        self.ingestor.ingestOperator(self.table_metadata, operator)

    def create_table(self):
        self.ingestor.createTable(self.table_metadata)
