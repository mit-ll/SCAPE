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
import sys
import logging
import struct
import StringIO
import json
import hashlib
import pprint
import base64
import types
from collections import namedtuple

from scape.registry.accumulo.proxy.ttypes import (
    ScanColumn, ScanOptions, BatchScanOptions,
    TimeType,  IteratorScope,
    )

from scape.registry.accumulo.exceptions import (
    ScapeAccumuloError,
    )

from scape.registry.accumulo.connection import Connection
from scape.registry.accumulo import iterators
from scape.registry.accumulo import scanner
from scape.registry.accumulo import writer

from scape.registry.accumulo.entities import (
    Cell, Row, Column, Family,
    )

_log = logging.getLogger('scape.registry.accumulo.table')
_log.addHandler(logging.NullHandler())

scope_map = {'scan': IteratorScope.SCAN,
             'minc': IteratorScope.MINC,
             'majc': IteratorScope.MAJC}
scope_map_inv = {v:k for k,v in scope_map.items()}
scope_map.update({v:v for k,v in scope_map.items()})

class ScopeSet(set):
    def __init__(self,scopes={'scan','minc','majc'}):
        self.update(scopes)

    @property
    def scopes(self):
        return {scope_map[v] for v in self}

class IteratorDict(dict):
    def __init__(self,table):
        D = table.client.listIterators(table.login,table.name)
        for it_name,scopes in D.items():
            sdict = self.setdefault(it_name,{})
            for scope in scopes:
                sdict[scope_map_inv[scope]] = table.client.getIteratorSetting(
                    table.login, table.name, it_name, scope
                )
        
class Table(object):
    ''' Representation of an Accumulo table

    Given a table name and an optional Connection object, provide
    '''
    def __init__(self,name, connection=None):
        self.name = name

        if connection:
            if isinstance(connection,Connection):
                self._connection = connection
            else:
                raise ScapeAccumuloError(
                    'Must pass instance of Connection object'
                    ' to initialize table with existing'
                    ' connection.')

    def __str__(self):
        return '<{cls}: {name} on host: {host}>'.format(
            cls=self.__class__.__name__,
            name=self.name, host=':'.join(map(str,[self.connection.host,
                                                   self.connection.port])),
            )

    def __repr__(self):
        return str(self)

    def __del__(self):
        _log.debug('__del__ %s',self.__class__)
        self.close()
        self._connection = None

    def close(self):
        if self._connection:
            self.connection.close()

    _connection = None
    @property
    def connection(self):
        if self._connection is None:
            self._connection = Connection()
        return self._connection
    def reset_connection(self):
        self.connection.reset()

    @property
    def login(self):
        return self.connection.login

    @property
    def client(self):
        return self.connection.client

    @property
    def exists(self):
        return self.client.tableExists(self.login, self.name)

    @property
    def splits(self):
        return self.client.listSplits(self.login, self.name, sys.maxint)

    def add_splits(self,splits):
        self.client.addSplits(self.login, self.name, splits)

    @property
    def iterators(self):
        return IteratorDict(self)

    def attach_iterator(self,iterator,scopes={'minc','majc','scan'}):
        self.client.attachIterator(
            self.login, self.name, iterator.setting, ScopeSet(scopes).scopes,
        )
    
    def remove_iterator(self,name,scopes={'minc','majc','scan'}):
        to_remove = (ScopeSet(scopes).scopes &
                     ScopeSet(self.iterators[name]).scopes)
        self.client.removeIterator(
            self.login, self.name, name, to_remove,
        )
        

    def create(self):
        if not self.exists:
            self.client.createTable(self.login, self.name, True,
                                    TimeType.MILLIS)
            _log.info('Table {} created'.format(self.name))
        else:
            _log.warn('Table {} already exists'.format(self.name))
            
    def destroy(self):
        if self.exists:
            self.client.deleteTable(self.login, self.name)
            _log.info('Table {} destroyed'.format(self.name))
        else:
            _log.warn('Table {} does not exist'.format(self.name))

    def rename(self,new_name):
        self.client.renameTable(self.login, self.name, new_name)
        self.name = new_name

    def scanner(self, **kw):
        return scanner.Scanner(self,**kw)
        

    def batch_scanner(self, **kw):
        return scanner.BatchScanner(self,**kw)

    def batch_writer(self, **kw):
        return writer.BatchWriter(self,**kw)

    def row_factory(self, **kw):
        return RowFactory(self,**kw)
    rowFactory = row_factory
        
    def delete_rows(self, table, srow, erow):
        self.client.deleteRows(self.login, table, srow, erow)


class RowFactory(object):
    def __init__(self,table,**kw):
        self.table = table
        self._batch_writer_kw = kw
        self._rows = []

    def __del__(self):
        _log.debug('__del__ %s',self.__class__)
        self.close()
        self.table = None
        
    def __enter__(self):
        return self
    def __exit__(self,type,value,traceback):
        _log.debug('__exit__ %s',self.__class__)
        self.close()

    def __len__(self):
        return len(self._rows)

    def close(self):
        self.flush()
        if self._writer:
            self._writer.close()
            self._writer = None

    def flush(self):
        if self._rows:
            _log.debug('flushing rows')
            for row in self._rows:
                self.write_row(row)
            self._rows = []
            self.writer.flush()

    _writer = None
    @property
    def writer(self):
        if self._writer is None:
            self._writer = self.table.batch_writer(
                **self._batch_writer_kw
                )
        return self._writer

    def key_row(self,row):
        try:
            S = json.dumps(row,sort_keys=True)
        except:
            S = pprint.pformat(row)
        D = hashlib.sha1(S).digest()
        return base64.b64encode(D)

    def write_row(self,row):
        if not row.key:
            row.key = self.key_row(row)
        self.writer.write_row(row)
        
    def new_row(self,*a,**kw):
        row = Row()
        self._rows.append(row)
        return row
    newRow = new_row

