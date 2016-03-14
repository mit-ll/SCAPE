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

''' Helper objects and functions for the Registry submodule
'''
import re
import inspect

from scape.utils import (
    lines, new_log,
    )

from scape.registry.exceptions import (
    ScapeRegistryError
)

_log = new_log('scape.registry.utils')

def sanitize_kw(kw):
    '''For dimensions that might conflict with reserved words, a trailing
    _ can be added, and this function will strip that _

    '''
    kw = kw.copy()
    for k,v in list(kw.items()):
        if k.endswith('_'):
            del kw[k]
            kw[k[:-1]] = v
    return kw

class HashableDict(dict):
    ''' A hashable dictionary

    Hashes on sorted (key,value) item tuple.
    '''
    def _key(self):
        return tuple((k,self[k]) for k in sorted(self))
    def __hash__(self):
        return hash(self._key())
    def __eq__(self,other):
        if isinstance(other,HashableDict):
            return self._key() == other._key()
        else:
            return self == other

    def __setitem__(self,key,value):
        raise AttributeError('HashableDict is immutable')
    def pop(self,key):
        raise AttributeError('HashableDict is immutable')
    def popitem(self,key):
        raise AttributeError('HashableDict is immutable')
    def clear(self):
        raise AttributeError('HashableDict is immutable')
        


class TaggedDimension(object):
    '''Hashable object representation of a sequence of zero-or-more tags
    followed by zero-or-one dimension

    Can be initialized with:

    1. Sequence of strings, where the first N-1 strings are tags and
    the Nth string is the dimension. For tags with no dimension,
    provide an empty string in the last place.

    2. String of the forms:

       "tag1:tag2:...:tagN:dim"

       "dim"

       "tag1:tag2:...:tagN:"

    Notice the trailing colon in the last example, as this is how you
    specify a set of tags with no dimension.

    '''
    def __init__(self,d):
        if type(d) in (list,tuple):
            elements = d
        else:
            strd = str(d)
            if ':' in strd:
                elements = strd.split(':')
            elif '__' in strd:
                elements = strd.split('__')
            else:
                elements = [strd]
        self.dim = elements[-1]
        self.tags = frozenset(elements[:-1])
        self.d = str(self)
    @classmethod
    def from_tags_dim(cls, tags, dim):
        return cls(':'.join(sorted(tags)+[dim]))
        
    def _key(self):
        return tuple(sorted(self.tags)+[self.dim])
    def __str__(self):
        return ':'.join(self._key())
    def __repr__(self):
        return repr(str(self))
    def __hash__(self):
        return hash(str(self))
    def __eq__(self,other):
        if isinstance(other,TaggedDimension):
            return ( (self.dim==other.dim) and (self.tags==other.tags) )
        elif isinstance(other,str):
            return self == TaggedDimension(other)
        return self==other
    def resolve(self,selection):
        tags = selection.tags(*self.tags).names
        dim = selection.dims(self.dim).node.get('name','') if self.dim else ''
        return TaggedDimension.from_tags_dim(tags,dim)

    
