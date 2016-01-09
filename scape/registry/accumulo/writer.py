"""
Copyright (2016) Massachusetts Institute of Technology.  Reproduction/Use 
of all or any part of this material shall acknowledge the MIT Lincoln 
Laboratory as the source under the sponsorship of the US Air Force 
Contract No. FA8721-05-C-0002.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
import logging
from collections import namedtuple

from scape.utils import (
    lines,
    )

from .proxy import ttypes
from .proxy.ttypes import (
    WriterOptions,
    )

from .entities import (
    Column, Row,
    )

_log = logging.getLogger('scape.registry.accumulo.writer')
_log.addHandler(logging.NullHandler())


_ColumnUpdate = namedtuple("ColumnUpdate", ['family','qualifier', 'visibility',
                                            'timestamp','delete','value'])
class ColumnUpdate(_ColumnUpdate):
    def __new__(cls, family='', qualifier='', visibility=None,
                timestamp=None, delete=None, value=''):
        return super(ColumnUpdate, cls).__new__(cls,family,qualifier,
                                                visibility,timestamp,delete,
                                                value)

    @property
    def ttype(self):
        return ttypes.ColumnUpdate(
            colFamily = self.family,
            colQualifier = self.qualifier,
            colVisibility = self.visibility,
            timestamp = self.timestamp,
            deleteCell = self.delete,
            value = self.value, 
        )
        

class Mutations(dict):
    ''' Dictionary of (rowkey, ColumnUpdate list) pairs
    '''
    @classmethod
    def from_rowdict(cls,mutations):
        '''Given a dictionary of row mutations of the form:

        {'rowkey0': {
           'fam0': {
             'qual': 'value0',
             Column('qual','vis0&vis1'): 'value1',
           },
           'fam1': {
             'qual': 'value2',
             Column('qual','vis0|vis1'): 'value3',
           },
         'rowkey1': {
           'fam0': {
             'qual': 'value4',
             Column('qual','vis0'): 'value5',
           },
          }
        }

        This produces a dictionary of ColumnUpdate objects that can be
        sent to the thrift proxy for ingestion.
        '''
        D = cls()
        for rowkey in mutations:
            D.add_row(rowkey,mutations[rowkey])
        return D

    def add_row(self,key,row):
        updates = self.setdefault(key,[])
        for family in row:
            famdict = row[family]

            for qualifier in famdict:
                value = famdict[qualifier]

                if isinstance(value,basestring):
                    # Map unicode to bytes by latin1 (iso-8859) encoding
                    if type(value) is unicode:
                        val = value.encode('latin-1')
                    else:
                        val = value
                else:
                    val = str(val)
                    
                # qualifier can be given in any form taken by the
                # from_obj classmethod of Column. Normally given as
                # just a string.
                column = Column.from_obj(qualifier)

                # update column with visibility and timestamp from
                # value object
                for attr in ['visibility','timestamp']:
                    if getattr(value,attr):
                        setattr(column,att,getattr(value,attr))

                update = ColumnUpdate(
                    family=family, value=val,
                    **column._asdict()
                    )
                updates.append(update.ttype)

    @classmethod
    def from_row(cls, row):
        D = cls()
        D.add_row(row.key,row)
        return D

    @classmethod
    def from_rows(cls, rows):
        D = cls()
        for row in rows:
            D.add_row(row.key,row)
        return D
        
class BatchWriter(object):
    """docstring for BatchWriter"""
    def __init__(self, table, max_memory=10*1024, latency_ms=30*1000,
                 timeout_ms=5*1000, threads=10):

        self.table = table

        self.max_memory = max_memory
        self.latency_ms = latency_ms
        self.timeout_ms = timeout_ms
        self.threads = threads

    def __enter__(self):
        return self

    def __exit__(self,*a,**kw):
        self.flush()
        self.close()

    def __del__(self):
        _log.debug('__del__ %s',self.__class__)
        self.close()
        self.table = None

    def close(self):
        if self.connected:
            try:
                self.table.client.closeWriter(self._writer)
            except:
                _log.error('Could not close writer')
            self._writer = None

    @property
    def connected(self):
        return bool(self._writer)

    @property
    def options(self):
        options = WriterOptions(
            maxMemory=self.max_memory,
            latencyMs=self.latency_ms,
            timeoutMs=self.timeout_ms,
            threads=self.threads,
        )
        return options

    _writer = None
    @property
    def writer(self):
        if self._writer is None:
            self._writer = self.table.client.createWriter(
                self.table.login, self.table.name, self.options
            )
        return self._writer

    @property
    def connected(self):
        return self._writer is not None

    def write_row(self,row):
        mutations = Mutations.from_row(row)
        self.write_mutations(mutations)

    def write_rows(self,rows):
        mutations = Mutations.from_rows(rows)
        self.write_mutations(mutations)

    def write_mutations(self,mutations):
        self.table.client.update(self.writer, mutations)

    def flush(self):
        try:
            self.table.client.flush(self.writer)
        except:
            _log.error('Unable to flush writer')
