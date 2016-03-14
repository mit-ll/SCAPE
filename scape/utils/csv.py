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

''' scape.utils.csv

Utilities for dealing with CSV files
'''

import sys
import csv
from datetime import datetime

from scape.utils.file import zip_open
from scape.utils.log import new_log

__all__ = ['CSVRow', 'ScapeCsvReaderError', 'csv_reader', 'csv_rows']

_log = new_log('scape.utils.csv')

csv.field_size_limit(sys.maxsize)

class CSVRow(dict):
    ''' Dictionary representation of a row from a CSV file

    columns is an order sequence of column names
    '''
    def __init__(self,columns,index=None, *a,**kw):
        dict.__init__(self,*a,**kw)
        self.columns = columns
        self.index = index

    def to_list(self):
        return [self.get(c,'') for c in self.columns]

class ScapeCsvReaderError(Exception):
    pass
    
def _csv_reader(rfp,columns=None,header=True,**kw):
    '''Given file-like object rfp, return generator for (row index, row
    dictionary) pairs (a la enumerate)

    '''
    if columns is None:
        if not header:
            raise ScapeCsvReaderError('''
            If there is no header for this CSV file, then columns must
            be provided
            ''')
        cols = next(csv.reader(rfp,**kw))
    else:
        cols = columns
        if header:
            next(csv.reader(rfp,**kw))

    reader = csv.DictReader(rfp,cols,**kw)
    start = datetime.now()
    t = start

    for i,row in enumerate(reader):
        if i%10000==0:
            now = datetime.now()
            _log.info('\n  row: %s %s %s',i,now-t,now-start)
            t = now
        yield i,CSVRow(cols,i,row)

def csv_reader(path,columns=None,header=True,**kw):
    '''Given path to (possibly compressed) CSV file (or file-like object),
    return generator for (row index, row dictionary) pairs (a la
    enumerate)

    '''
    if isinstance(path,str):
        with zip_open(path,'rbU') as rfp:
            for i,row in _csv_reader(rfp,columns,header,**kw):
                yield i,row

    else:
        rfp = path
        for i,row in _csv_reader(rfp,**kw):
            yield i,row

def csv_rows(path,columns=None,header=True,**kw):
    '''Given path to CSV file (or file-like object), return generator for
    row dictionaries

    '''
    for _,row in csv_reader(path,columns,header,**kw):
        yield row


