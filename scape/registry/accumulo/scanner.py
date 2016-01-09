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
import collections
import traceback
from copy import deepcopy
from collections import namedtuple


from scape.utils import (
    lines,
    )
from .proxy import ttypes
from .proxy.ttypes import (
    ScanColumn, ScanOptions, BatchScanOptions,
    IteratorSetting, 
    )

from .utils import (
    transform_columns as _transform_columns
    )

from .exceptions import (
    ScapeAccumuloError,
    )
from .entities import (
    Cell, Row,
    )
from .iterators import (
    BaseIterator, WholeRowIterator, EventWholeRowIterator,
    )

_log = logging.getLogger('scape.registry.accumulo.scanner')
_log.addHandler(logging.NullHandler())

def _following_array(val):
    if val:
        return val+"\0"
    else:
        return None

def _prior_row(key):
    if key:
        return key[:-1] + chr(ord(key[-1])-1)
    return key


_Extent = namedtuple("Extent", ['row', 'family','qualifier','visibility',
                                'timestamp', 'inclusive'])
class Extent(_Extent):
    ''' Scan range extent (start or stop condition) as a namedtuple
 
    '''
    def __new__(cls, row=ttypes.Key.thrift_spec[1][-1],
                family=ttypes.Key.thrift_spec[2][-1],
                qualifier=ttypes.Key.thrift_spec[3][-1],
                visibility=ttypes.Key.thrift_spec[4][-1],
                timestamp=ttypes.Key.thrift_spec[5][-1],
                inclusive=True):
        return super(Extent, cls).__new__(cls,row,family,qualifier,visibility,
                                          timestamp,inclusive)

    @classmethod
    def from_obj(cls, extent):
        '''Create an Extent object from a Python object

        If the given object is a string, interpret as row key

        If the given object is a sequence, interpret as positional
        arguments

        If a dictionary, interpret as keyword arguments

        Otherwise check to make sure that it's already an Extent
        object.

        If None, then return a default Extent object.

        '''
        if extent:
            if isinstance(extent,basestring):
                extent = cls(row=extent)
            if isinstance(extent,dict):
                extent = cls(**extent)
            elif isinstance(extent,(list,tuple)):
                extent = cls(*extent)
            elif not isinstance(extent,cls):
                raise ScapeAccumuloError(
                    'range extent must be given as'
                    ' sequence, dictionary or Extent object')
        else:
            extent = cls()
        return extent

    def __str__(self):
        vals = ' '.join(['{k}:{v}'.format(k=k,v=v) for k,v in self.items()])
        return '<Extent: {vals} >'.format(vals=vals)

    def items(self):
        return zip(self._fields,self)


class Range(object):
    ''' Scan range object


    '''

    def __init__(self,start=None, stop=None):
        self.start, self.stop = map(Extent.from_obj,[start,stop])

    def __str__(self):
        rstr = '<Range start: {start}\n       stop: {stop}>'.format(
            start=self.start, stop=self.stop
        )
        return rstr

    @classmethod
    def from_obj(cls, obj):
        '''Generate a range from a Python object following the following
        specs:
        
        E.g.

        'row00' --> fetch row 'row00'

        ['row00','row01'] --> fetch range between keys "row00" and "row01"

        [('row00','fam0','qual0'), ('row00','fam0','qual1')] --> fetch
        contents of row 'row00' between 'qual0' and 'qual1' for family
        'fam0'

        [{'row':'row00','family':'famA'},
         {'row':'row00','family':'famX','inclusive':False}] --> fetch
         contents for 'row00' for all families between 'famA' and
         'famX', not including 'famX'

        '''
        if isinstance(obj, basestring):
            return cls(Extent(row=obj),Extent(row=obj))
        elif isinstance(obj, Range):
            return obj
        elif obj is None:
            return obj
        else:
            return cls(*map(Extent.from_obj,obj))
        
    @property
    def ttype(self):
        #_log.debug(self)
        r = ttypes.Range()

        r.startInclusive = self.start.inclusive
        r.stopInclusive = self.stop.inclusive

        def get_key(kd):
            return ttypes.Key(
                row = kd.row,
                colFamily = kd.family,
                colQualifier = kd.qualifier,
                colVisibility = kd.visibility,
                timestamp = kd.timestamp,
            )

        if self.start.row:
            r.start = get_key(self.start)

            if not self.start.inclusive:
                # BDO: I'm puzzled by why this is here if
                # startInclusive is relevant...
                r.start.row = _following_array(r.start.row)
            
        if self.stop.row:
            r.stop = get_key(self.stop)
            if self.stop.inclusive:
                # BDO: I'm puzzled by why this is here if
                # stopInclusive is relevant...
                r.stop.row = _following_array(r.stop.row)


        #_log.debug(r)
        return r


def _process_iterator(iterator):
    if isinstance(iterator, IteratorSetting):
        return iterator
    elif isinstance(iterator, BaseIterator):
        return iterator.setting
    else:
        raise ScapeAccumuloError(
            "Cannot process iterator: {}".format(iterator)
        )

def _get_scan_columns(cols):
    # Convert column list into list of ScanColumn objects
    columns = None
    if cols:
        columns = []
        for col in cols:
            sc = ScanColumn()
            sc.colFamily = col[0]
            sc.colQualifier = col[1] if len(col) > 1 else None
            columns.append(sc)
    return columns

class Scanner(object):
    ''' doc for Scanner
    '''
    row_class = Row
    def __init__(self, table, scanrange=None, columns=None,
                 auths=None, iterators=None, bufsize=None, batchsize=10):
        self.table = table
        self.scanrange = scanrange
        self.columns = columns
        self.auths = auths
        self.iterators = iterators
        self.bufsize = bufsize
        self.batchsize = batchsize

    def __del__(self):
        _log.debug('__del__ %s',self.__class__.__name__)
        self.close()
        self.table = None

    def __enter__(self):
        return self

    def __exit__(self,*a,**kw):
        self.close()

    def close(self):
        if self._scanner:
            try:
                self.table.client.closeScanner(self.scanner)
            except:
                _log.error(lines('Could not close scanner',
                                 traceback.format_exc()))
                                 
            self._scanner = None
            self._cells = None
            self._cell_batches = None
            self._rows = None
            self._row_batches = None

    _scanrange = None
    @property
    def scanrange(self):
        if self._scanrange:
            return Range.from_obj(self._scanrange).ttype

    @scanrange.setter
    def scanrange(self,scanrange):
        self._scanrange = scanrange

    _iterators = None
    @property
    def iterators(self):
        if self._iterators:
            self._iterators = [_process_iterator(i) for i in self._iterators]
            return self._iterators
    @iterators.setter
    def iterators(self,iterators):
        self._iterators = iterators

    _columns = None
    @property
    def columns(self):
        return _get_scan_columns(_transform_columns(self._columns))
    @columns.setter
    def columns(self,columns):
        self._columns = columns

    @property
    def options(self):
        options = ScanOptions(
            self.auths,
            self.scanrange,
            self.columns,
            self.iterators,
            self.bufsize,
        )
        return options

    _scanner = None
    @property
    def scanner(self):
        if self._scanner is None:
            self._scanner = self.table.client.createScanner(
                self.table.login, self.table.name, self.options,
            )
        return self._scanner

    def _cell_result_individual(self,results):
        for e in results.results:
            yield Cell.from_scan_entry(e)
            
    def _cell_result_batch(self,results):
        yield (Cell.from_scan_entry(e) for e in results.results)

    def _cell_generator_for_handler(self,result_handler):
        BS = self.batchsize
        maxBS = 100
        try:
            while True:
                nBS = int(BS*1.5)
                BS = maxBS if nBS>maxBS else nBS
                results = self.table.client.nextK(
                    self.scanner, BS
                )

                for obj in result_handler(results):
                    yield obj

                if not results.more:
                    raise StopIteration
        finally:
            self.close()


    _cells = None
    @property
    def cells(self):
        if self._cells is None:
            self._cells = self._cell_generator_for_handler(
                self._cell_result_individual
            )
        return self._cells

    _cell_batches = None
    @property
    def cell_batches(self):
        if self._cell_batches is None:
            self._cell_batches = self._cell_generator_for_handler(
                self._cell_result_batch
            )
        return self._cell_batches

    _wri = None
    @property
    def whole_row_iterator(self):
        if self._wri is None:
            self._wri = EventWholeRowIterator()
        return self._wri
            
    def _row_generator_for_cell_handler(self, cell_handler):
        if self.iterators:
            priorities = [it.priority for it in self.iterators
                          if it.priority is not None]
            priority = max(priorities)+1
            self.whole_row_iterator.priority = priority
            _log.debug('Setting WRI priority: %s',priority)
            self.iterators.append(self.whole_row_iterator)
        else:
            priority = 50
            self.whole_row_iterator.priority = priority
            _log.debug('Setting WRI priority: %s',priority)
            self.iterators = [self.whole_row_iterator]

        try:
            for obj in cell_handler():
                yield obj
        finally:
            self.close()

    def _rows_from_individual_cells(self):
        for cell in self.cells:
            row = self.row_class.from_whole_row_cell(cell)
            yield row
    
    def _rows_from_cell_batches(self):
        for cells in self.cell_batches:
            row_batch = self.row_class.from_whole_row_cells(cells)
            yield row_batch
    
    _rows = None
    @property
    def rows(self):
        if self._rows is None:
            self._rows = self._row_generator_for_cell_handler(
                self._rows_from_individual_cells
            ) 
        return self._rows

    _row_batches = None
    @property
    def row_batches(self):
        if self._row_batches is None:
            self._row_batches = self._row_generator_for_cell_handler(
                self._rows_from_cell_batches
            )
        return self._row_batches

            
class BatchScanner(Scanner):
    ''' doc for BatchScanner
    '''
    def __init__(self, table, scanranges=None, columns=None,
                 auths=None, iterators=None, 
                 numthreads=None, batchsize=10):
        self.table = table
        self.scanranges = scanranges
        self.columns = columns
        self.auths = auths
        self.iterators = iterators
        self.numthreads = numthreads
        self.batchsize = batchsize

    @property
    def options(self):
        options = BatchScanOptions(
            self.auths,
            self.scanranges,
            self.columns,
            self.iterators,
            self.numthreads,
        )
        return options

    _scanranges = None
    @property
    def scanranges(self):
        if self._scanranges:
            scanranges = [Range.from_obj(sr).ttype for sr in self._scanranges]
            return scanranges

    @scanranges.setter
    def scanranges(self,scanranges):
        self._scanranges = scanranges

    @property
    def scanner(self):
        if self._scanner is None:
            self._scanner = self.table.client.createBatchScanner(
                self.table.login, self.table.name, self.options,
            )
        return self._scanner

