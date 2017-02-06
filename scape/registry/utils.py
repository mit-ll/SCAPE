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

from six import string_types

from .field import Field
from .tagged_dim import TaggedDim, tagged_dim

def field_or_tagged_dim(x):
    ''' Type routing function for Field or TagDims information
    '''
    if x and isinstance(x, string_types):
        x = x.strip()
        if x.startswith("@"):
            return Field(x[1:])
        else:
            return tagged_dim(x)
    elif isinstance(x, Field):
        return x
    elif isinstance(x, TaggedDim):
        return x
    elif x and type(x) in (list, tuple):
        return tagged_dim(x)
    else:
        raise ValueError(
            "Expecting Field, TaggedDim, string or list of strings,"
            " not {} of type {}".format(x,type(x))
        )
field_or_tagged_dim = field_or_tagged_dim

