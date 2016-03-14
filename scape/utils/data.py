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

''' scape.utils.data

Utilities for dealing with Python objects and data generally.
'''
import math

from scape.utils.log import new_log

__all__ = ['sortd', 'ndigits', 'nzf', 'zf', 'ZF', 'enumerate_reverse']

_log = new_log('scape.utils.data')

def sortd(d):
    ''' return dictionary items sorted by value
    '''
    return sorted(list(d.items()),key=lambda e:e[-1])

def ndigits(N):
    '''Given some number N return number of digits in base10. Negative
    numbers return number of digits + 1 (for negative sign).

    '''
    if N == 0:
        nd = 1
    else:
        nd = int(math.log10(abs(N)))+1
        if N < 0:
            nd += 1
    return nd
    
def nzf(N):
    '''Given some max number N, find the correct zfill value (number of
    digits in base10)

    '''
    return ndigits(N)

def zf(v):
    ''' return string version of v zero-padded by 2
    '''
    return str(v).zfill(2)

class ZF(object):
    '''zero-padding object

    max_value is the maximum value represented and thus determines the
    number of digits (the zfill parameter)

    >>> z = ZF(1000)
    >>> z(23)
    '0023'
    >>> z(999)
    '0999'
    >>> z(1000)
    '1000'

    If max_value is negative, then the zfill parameter is incremented
    by one to maintain consistent width across all values.
    
    >>> z = ZF(-1000)
    >>> z(-9)
    '-0009'
    >>> z(-999)
    '-0999'
    >>> z(-1000)
    '-1000'

    '''
    def __init__(self,max_value):
        self.max_value = max_value

    @property
    def ndigits(self):
        return ndigits(self.max_value)

    def __call__(self,v):
        return str(v).zfill(self.ndigits)
    
def enumerate_reverse(seq,start=0):
    ''' reversed enumerate generator

    >>> list(enumerate_reverse(['a','b','c']))
    [(2, 'c'), (1, 'b'), (0, 'a')]
    '''
    for index in range(len(seq)-start-1,-1,-1):
        yield index,seq[index]

