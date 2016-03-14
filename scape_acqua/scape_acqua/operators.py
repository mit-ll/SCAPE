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

import abc
import collections

import acqua
import acqua.utils
import acqua.operators

from scape.utils.log import new_log
from scape.utils.decorators import (
    memoized_property
)

import scape.registry.operators

_log = new_log('scape_acqua.operators')


class AcquaOperatorSource(collections.Iterator):
    _operator_source = None
    @property
    def operator_source(self):
        return self._operator_source

    @operator_source.setter
    def operator_source(self, operator_source):
        if not acqua.utils.isjinstance(operator_source,acqua.OperatorSource):
            raise AttributeError(
                "{} is not a Java Acqua OperatorSource. The"
                " operator_source @property for Python"
                " OperatorSource wrappers must return an instance of"
                " edu.mit.ll.acqua.OperatorSource".format(operator_source)
            )
        self._operator_source = operator_source
        

    def next(self):
        if self.operator_source.hasNext():
            return self.resolve_row(
                acqua.utils.row_to_dict(self.operator_source.next())
            )
        raise StopIteration

    def resolve_row(self,row):
        return row

    def _next(self):
        if self.operator_source.hasNext():
            return self.operator_source.next()
        raise StopIteration

    @property
    def operator(self):
        return acqua.Operator.createOperator(self.operator_source)

    def __or__(self, operator):
        operator.source = self
        return operator

class _PythonIteratorSource(acqua.PythonOperatorSource):
    def __init__(self, iterator):
        super(_PythonIteratorSource,self).__init__()
        self.iterator = iter(iterator)
        self.current_row = None
        self.last_row = None
        self._has_next = True

    def _new_hash_map(self):
        return acqua.WeakHashMap().of_(acqua.String,acqua.String)

    def _iterate(self):
        try:
            if self.current_row is None:
                self.current_row = next(self.iterator)
        except StopIteration:
            self._has_next = False

    def hasNext(self):
        if self._has_next:
            self._iterate()
        return self._has_next

    def next(self):
        if self._has_next:
            self._iterate()
            hmap = acqua.utils.dict_to_row(self.current_row)
            self.last_row = self.current_row
            self.current_row = None
            return hmap

    def finishedPage(self):
        return True

class PythonSource(AcquaOperatorSource):
    '''
    '''
    def __init__(self, iterator_or_sequence):
        '''
        '''
        self.operator_source = _PythonIteratorSource(
            iterator_or_sequence
        )

class DummySource(AcquaOperatorSource):
    '''Dummy OperatorSource for testing and experimentation

    Generates a given number of rows, each with a given number of
    fields populated with random integer values

    '''
    def __init__(self, nrows=None, nfields=None, one_row_per_page=False,
                 seed=None, rows=None):
        if rows is None:
            if seed is None:
                self.rows = acqua.DummySource.sourceData(nrows, nfields)
            else:
                self.rows = acqua.DummySource.sourceData(nrows, nfields, seed)
        else:
            self.rows = acqua.ArrayList()
            for row in rows:
                self.rows.add(acqua.utils.dict_to_row(row))
            
        self.operator_source = acqua.DummySource(
            self.rows, one_row_per_page,
        )

    def rows_as_list(self):
        return map(acqua.utils.row_to_dict,self.rows)
        

class CSVScanner(AcquaOperatorSource):
    '''CSV file parser OperatorSource

    Can be used as an OperatorSource for starting Operator chains, or
    can be converted to an operator for use by an Ingestor.

    '''
    def __init__(self, files, fields=None):
        '''CSV file parser

        Args:

          files (seq): sequence of paths to parse

          fields (seq): sequence of fields (str) to extract from each
            CSV file

        >>> S = CSVScanner(['file1.csv','file2.csv'],['colA','colB'])
        >>>
        '''
        files = acqua.utils.string_list(files)
        fields = acqua.utils.string_list(fields or [])
        self.operator_source = acqua.CSVScanner(files,fields)



class AcquaOperator(AcquaOperatorSource):
    factory = None
    source = None

    def __init__(self,*a,**kw):
        self.init_args = (a,kw)

    def copy(self):
        a,kw = self.init_args
        return self.__class__(*a,**kw)

    @property
    def operator_source(self):
        return self.operator

    _operator = None
    @property
    def operator(self):
        # First, connect the source to this operator (if it exists)
        if self.source is not None:
            self._operator.setSource(self.source.operator_source)
        return self._operator

    @operator.setter
    def operator(self,operator):
        if not acqua.utils.isjinstance(operator,acqua.Operator):
            raise AttributeError(
                "{} is not a Java Acqua Operator. The operator"
                " @property for Python Operator wrappers must be"
                " an instance of edu.mit.ll.acqua.Operator".format(operator)
            )
        self._operator = operator

    _iter = None
    def next(self):
        if self._iter is None:
            self._iter = iter(self.operator.eachRow())
        return self.resolve_row(
            acqua.utils.row_to_dict(next(self._iter))
        )

    def resolve_row(self,row):
        return row

    def __or__(self, operator):
        operator.source = self
        operator.factory = self.factory
        return operator

class AcquaSelect(AcquaOperator):
    '''Select AcquaOperator which represents a database query.

    '''
    @classmethod
    def from_factory(cls, operator, factory):
        new = cls()
        new.operator = operator
        new.factory = factory
        return new
        
#----------------------------------------------------------------------
# Removed the old Acqua Operator classes until a better integration
# solution can be found
#----------------------------------------------------------------------

__all__ = []

