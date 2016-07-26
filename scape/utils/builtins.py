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
''' Helper functions for builtin objects 
'''
from __future__ import absolute_import

import pprint
from collections import OrderedDict
from copy import deepcopy
import logging

# from scape.utils.log import new_log

__all__ = ['merge_dicts_ip','merge_dicts',]

# _log = new_log('scape.utils.builtins')
_log = logging.getLogger('scape.utils.builtins')
_log.addHandler(logging.NullHandler())

def merge_dicts_ip(*dicts):
    '''Merge source dictionaries with dest dictionary **in place**

    The first dictionary in the argument list is always the dest
    dictionary. The rest of the arguments (sources) are merged, in
    order from left to right, with the dest.

    Assumtions:

    - dest and sources can be arbitrarily nested, but the only value
      data types handled are list, tuple and set.

    Rules:

    - If dest[k] and source[k] are not the same type, then

      - If dest[k] is a tuple and source[k] is a list, then dest[k]
        becomes a list with source[k] added to it

      - Otherwise, dest[k] is overwritten by source[k]

    - If dest[k] and sources[k] are not sets, lists or dicts,
      then dest[k] is overwritten by sources[k].

    - If dest[k] and sources[k] are lists, then the values in
      sources[k] are added to the end of dest[k].

    - If dest[k] and sources[k] are sets, then they are unioned
      together

    Otherwise (i.e. dest[k] and sources[k] are dicts), the process
    traverses down the nested hierarchy

    >>> merge_dicts({'a':[1]},{'a':[1,2]})
    {'a': [1, 1, 2]}
    >>> merge_dicts({'a':(1,)},{'a':(1,2)})
    {'a': (1, 1, 2)}
    >>> merge_dicts({'a':(1,)},{'a':[1,2]})
    {'a': [1, 1, 2]}
    >>> merge_dicts({'a':[1,2]},{'a':(3,4)})
    {'a': (3, 4)}
    >>> merge_dicts({'a':{1}},{'a':{1,2}})
    {'a': {1, 2}}
    >>> merge_dicts({'a':{'b':4}},{'a':{'b':8}})
    {'a': {'b': 8}}

    '''

    dest, sources = dicts[0], dicts[1:]
    for source in sources:
        stack = [(dest,source)]
        while stack:
            dst,src = stack.pop()
            try:
                for k in src:
                    if k not in dst:
                        dst[k] = src[k]
                    else:
                        if not isinstance(src[k],(list,set,dict)):
                            dst[k] = src[k]
                        elif isinstance(src[k], list):
                            if isinstance(dst[k], tuple):
                                dst[k] = list(dst[k])
                            elif not isinstance(dst[k], list):
                                dst[k] = []
                            dst[k].extend(src[k])
                        elif isinstance(src[k], set):
                            dst[k] |= src[k]
                        else:
                            #_log.debug('key:%s src[k]:%s',k,src[k])
                            stack.append((dst[k],src[k]))
            except:
                _log.error(
                    'Error merging dictionaries: \n'
                    'dest: {} \n'
                    'source: {}'.format(
                        dest, source,
                    )
                )
                raise
    return dest

def merge_dicts(*dicts):
    '''Merge source dictionaries (second argument on) into dest dictionary
    (first argument) without side effects. See merge_dicts_ip for more
    details.

    '''
    dicts = list(dicts)
    dicts[0] = deepcopy(dicts[0])
    return merge_dicts_ip(*dicts)

def odict_to_dict(odict):
    '''Convert OrderedDict to normal dictionary recursively

    '''
    new_dict = dict(odict)
    stack = [new_dict]
    while stack:
        new = stack.pop()
        for key, value in list(new.items()):
            if isinstance(value, OrderedDict):
                new[key] = dict(value)
                stack.append(new[key])
    return new_dict
