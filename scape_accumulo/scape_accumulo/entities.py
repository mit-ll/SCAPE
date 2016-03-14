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

import logging
import struct
import StringIO
from collections import namedtuple
from datetime import datetime

from .exceptions import ScapeAccumuloError

_log = logging.getLogger('scape_accumulo.entities')
_log.addHandler(logging.NullHandler())

# namedtuple with defaults
# http://stackoverflow.com/a/16721002
_Cell = namedtuple("Cell", ['row', 'family','qualifier',
                            'visibility', 'timestamp', 'value'])
class Cell(_Cell):
    ''' Full 6-tuple Accumulo cell as a namedtuple
 
    '''
    def __new__(cls, row, family='', qualifier='', visibility=None,
                timestamp=None, value=''):
        return super(Cell, cls).__new__(cls,row,family,qualifier,visibility,
                                        timestamp,value)

    @classmethod
    def from_scan_entry(cls,entry):
        cell = cls(entry.key.row, entry.key.colFamily, entry.key.colQualifier,
                   entry.key.colVisibility, entry.key.timestamp, entry.value)
        return cell

_Column = namedtuple("Column", ['qualifier', 'visibility','timestamp','delete'])
class Column(_Column):
    ''' Representation of column metadata as immutable namedtuple

    The convention is to represent rows as a dictionary:

    E.g. {'fam': {'cq': 'val' } }

    for "plain vanilla" rows.
    
    For rows with column qualifiers that have visibilities, assigned
    timestamps or will eventually be deletion operations, the Column
    object provides the ability to model this. 
    
    E.g. {'fam': { Column('cq','(A&B)|C'): 'val' } }
 
    '''
    def __new__(cls, qualifier='', visibility=None,timestamp=None, delete=None):
        return super(Column, cls).__new__(cls,qualifier,visibility,
                                          timestamp,delete)

    @classmethod
    def from_obj(cls,obj):
        '''Create Column object from 4 different types of objects: string
        specifying column qualifier, Column object (no-op), sequence
        of args or dictionary of kw args

        '''
        def is_seq(o):
            seq = False
            try:
                len(o)
                seq = True
            finally:
                return seq
                
        if isinstance(obj, basestring):
            return cls(qualifier=obj)
        elif isinstance(obj, cls):
            return obj
        elif is_seq(obj):
            return cls.from_seq(obj)
        elif isinstance(obj,dict):
            return cls.from_dict(obj)

    @classmethod
    def from_seq(cls,ctup):
        return cls(*ctup)

    @classmethod
    def from_dict(cls,cdict):
        return cls(**cdict)

class Value(bytes):
    '''Subclass of bytes type to represent values in Accumulo.

    We want to maintain the abstraction that values are
    strings/bytearrays, but they also have visibilities and timestamps
    associated with them. So, we add this metadata as instance
    attributes.

    '''
    visibility = None
    timestamp = None
    def __new__(cls, value, visibility=visibility, timestamp=timestamp):
        if isinstance(value,cls):
            return value
        if type(value) is unicode:
            # Map unicode to bytes by latin1 (iso-8859) encoding
            value = value.encode('latin-1')
        elif not isinstance(value,basestring):
            # Make sure value is of type bytes
            value = bytes(value)
        instance = super(Value,cls).__new__(cls,value)
        instance.visibility = visibility
        instance.timestamp = timestamp
        return instance

    @property
    def datetime(self):
        if self.timestamp:
            return datetime.fromtimestamp(self.timestamp)

class Family(dict):
    '''(column family, column qualifier[, visibility, timestamp], value)
    tuples represented as a single dictionary

    Keys can be two types of objects: strings and Column objects

    Accessing values associated with either column qualifier strings
    or Column objects can resolve to either:

    1) a single Value object (when only one unique column
       qualifier/Column key exists within this column family) or 

    2) a list of Value objects (when multiple Column keys exist)

    The latter will only happen when multiple values for a particular
    column qualifier are returned (e.g. values associated with
    multiple visibility classifications and/or multiple timestamps) by
    a scanner.
    '''
    def __init__(self,**data):
        self._qualifiers = {}

        for k,v in data.items():
            self[k] = v

    def __setitem__(self,key,value):
        if isinstance(key,Column):
            self._qualifiers.setdefault(key.qualifier,[]).append(key)
            value = Value(value, visibility=key.visibility,
                          timestamp=key.timestamp)
        else:
            value = Value(value)
        dict.__setitem__(self,key,value)

    def __getitem__(self,key):
        if key in self._qualifiers:
            columns = self._qualifiers[key]
            if key in self:
                # If the qualifier exists in this family dict as a
                # "vanilly" (qual,value) pair (i.e. without a
                # visibility/timestamp)
                columns = columns + dict.__getitem__(self,key)
            values = [dict.__getitem__(self,c) for c in columns]
            if len(values)==1:
                return values[0]
            else:
                return values
        else:
            return dict.__getitem__(self,key)
        
class Row(dict):
    '''(row, col family, col qualifier[, visibility, timestamp], value)
    6-tuples represented as a dictionary

    '''

    _key = None
    @property
    def key(self):
        return self._key

    @key.setter
    def key(self,key):
        self._key = key

    def _fam_qual(self,key):
        family, qualifier = key, None
        if isinstance(key,(list,tuple)):
            family, qualifier = key
        elif isinstance(key,basestring):
            if ':' in key:
                family, qualifier = key.split(':',1)
        return family, qualifier

    def __getitem__(self,key):
        '''Key can be given in two ways:

        1) row["fam0"] => returns Family object associated with "fam0"

        2) row["fam0:qual0"] => return Value object associated with
        column family "fam0" and column qualifier "qual0"

        '''
        family, qualifier = self._fam_qual(key)
        if qualifier is None:
            return dict.__getitem__(self,key)
        else:
            return dict.__getitem__(self,family)[qualifier]

    def __contains__(self,key):
        family, qualifier = self._fam_qual(key)
        if qualifier is None:
            return dict.__contains__(self,family)
        else:
            return qualifier in self.get(family,{})
                

    def get(self,key,default=None):
        family, qualifier = self._fam_qual(key)
        if qualifier is None:
            return dict.get(self,key,default)
        else:
            return dict.get(self,family,{}).get(qualifier,default)

    def __setitem__(self,key,value):
        family, qualifier = self._fam_qual(key)
        if qualifier is None:
            if not isinstance(value,dict):
                raise ScapeAccumuloError('When setting a column family,'
                                         ' the argument must be a dictionary')
            dict.__setitem__(self,family, Family(**value))
        else:
            self[family][qualifier] = value
            
        
    @classmethod
    def from_cells(cls, cells):
        '''Class method. Generator for rows from sequence of Cell objects. If
        these cells are being generated by a batch scanner (that isn't
        a whole-row scanner), then this will likely yield a fragmented
        set of rows. It's best to use this with a normal scanner. For
        batch scanners using a WholeRowIterator, use the
        from_whole_row_cells class method instead.
        '''
        row = None
        for cell in cells:
            key = cell.row
            if (row is None):
                row = cls()
                row.key = key

            elif (row and key != row.key):
                # Found a new row, yield current and start new. 
                yield row
                row = cls()
                row.key = key

            fdict = row.setdefault(cell.family,Family())

            value = Value(cell.value,visibility=cell.visibility,
                          timestamp=cell.timestamp)

            if not cell.visibility:
                # just a "vanilla" (column qualifier, value) pair, so
                # use the column qualifier as the key
                fdict[cell.qualifier] = value

            else:
                column = Column(
                    qualifier = cell.qualifier,
                    visibility = cell.visibility,
                    timestamp = cell.timestamp,
                    )

                fdict[column] = value
        if row:
            yield row

    @classmethod
    def from_whole_row_cell(cls, cell):
        '''Transliteration of "decodeRow" static method of WholeRowIterator
        into Python

        '''
        value = StringIO.StringIO(cell.value)
        numKeys = struct.unpack('!i',value.read(4))[0]
        row = cls()
        row.key = cell.row

        for i in range(numKeys):
            if value.pos == value.len:
                raise ScapeAccumuloError(
                    'Reached the end of the parsable string without'
                    ' having finished unpacking. Likely an error'
                    ' of passing a cell that is not from a'
                    ' WholeRowIterator.'
                    )
            cf = value.read(struct.unpack('!i',value.read(4))[0])
            fdict = row.setdefault(cf,Family())
            cq = value.read(struct.unpack('!i',value.read(4))[0])
            cv = value.read(struct.unpack('!i',value.read(4))[0])
            cts = struct.unpack('!q',value.read(8))[0]/1000.
            val = value.read(struct.unpack('!i',value.read(4))[0])
            if not cv:
                # just a "vanilla" (column qualifier, value) pair, so
                # use the column qualifier as the key
                key = cq
            else:
                key = Column(qualifier=cq,visibility=cv,timestamp=cts)
            fdict[key] = Value(val,visibility=cv,timestamp=cts)

        return row

            
    @classmethod
    def from_whole_row_cells(cls, cells):
        for cell in cells:
            yield cls.from_whole_row_cell(cell)
            
