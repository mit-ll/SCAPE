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

import functools
import types
import time
import datetime

import acqua

def string_list(seq):
    '''Return a Java ArrayList<String> of some sequence of objects

    If seq is already an ArrayList, then return it
    '''
    if isinstance(seq,acqua.ArrayList):
        return seq

    if seq is None:
        return acqua.ArrayList().of_(acqua.String)
        
    array = acqua.ArrayList().of_(acqua.String)
    for v in seq:
        array.add(str(v))
    return array

def date(dt):
    '''Return a Java Date object from a datetime object

    '''
    return acqua.Date(long(time.mktime(dt.timetuple())*1000))

def event_date(dt):
    '''Return an Acqua EventDate object from a datetime object

    '''
    return acqua.EventDate(date(dt))

def isjinstance(obj,*classes):
    if isinstance(obj,classes):
        return True
        
    if not hasattr(obj,'class_'):
        return False

    return any(c.class_.isAssignableFrom(obj.class_)
               for c in classes if hasattr(c,'class_'))

def row_to_dict(row):
    row = acqua.Map.cast_(row)
    return {k:row.get(k).toString() for k in row.keySet()}

def obj_to_str(obj):
    if isinstance(obj,basestring):
        if isinstance(obj,unicode):
            obj = obj.encode('latin-1','ignore')
    else:
        obj = str(obj)
    return obj
    
def dict_to_row(row):
    row_map = acqua.HashMap().of_(acqua.String,acqua.String)
    for key,value in row.items():
        if isinstance(value, dict):
            for k,v in value.items():
                k,v = map(obj_to_str,(k,v))
                row_map.put('{}:{}'.format(key,k), v)
        else:
            key,value = map(obj_to_str,(key,value))
            row_map.put(key,value)
    return row_map


