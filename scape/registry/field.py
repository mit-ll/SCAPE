# Copyright (2016) Massachusetts Institute of Technology.
# Reproduction/Use of all or any part of this material shall
# acknowledge the MIT Lincoln Laboratory as the source under the
# sponsorship of the US Air Force Contract No. FA8721-05-C-0002.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

from __future__ import absolute_import

from six import string_types

class Field(object):
    '''The  name of a field/column/cell in a data source.

    Fields represent the data-source-specific name for a particular
    individual element of data. Examples are columns names in SQL
    stores, cell names in NoSQL stores, etc.

    In Scape, they are associated (via the :class:`TableMetadata`
    object) with tags (semantic descriptors) and dimensions
    (domain-specific data types). The goal being to allow analysts to
    pose questions in terms of these tags and dimensions and have
    those questions be transformed automatically into well-formed data
    source queries.

    '''
    def __init__(self, name):
        self._name = name

    def __repr__(self):
        return "Field(" + repr(self._name) + ")"

    @property
    def name(self):
        ''' The name of the field '''
        return self._name

    def __eq__(self, other):
        return type(self) == type(other) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

def field(f):
    ''' Type routing function for field information
    '''
    if f and isinstance(f, string_types):
        return Field(f.replace('@',''))
    elif isinstance(f, Field):
        return f
    else:
        raise ValueError(
            "Expecting str or Field object"
            " not {} of type {}".format(f,type(f))
        )
