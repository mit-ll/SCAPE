# Copyright (2016) Massachusetts Institute of Technology.
# Reproduction/Use of all or any part of this material shall
# acknowledge the MIT Lincoln Laboratory as the source under the
# sponsorship of the US Air Force Contract No. FA8721-05-C-0002.

# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

import re
from nose.tools import *

from scape.registry.tag import Tag, tag
from scape.registry.dim import Dim, dim
from scape.registry.tagged_dim import TaggedDim, tagged_dim

# TaggedDim ##############################################################

@raises(ValueError)
def test_tagged_dim_str():
    TaggedDim(tags=["asdf"])

@raises(ValueError)
def test_tagged_dim_baddim():
    TaggedDim(dim=1)

@raises(ValueError)
def test_tagged_dim_badarg():
    tagged_dim(1)

def test_tagged_dim_repr():
    t = tagged_dim("tag1:dim")
    assert_equal( t , eval(repr(t)))

def test_tagged_dim_to_dict():
    t = tagged_dim("tag1:dim")
    assert_equal({'tags':['tag1'], 'dim':'dim'}, t.to_dict())

def test_parse_tagged_dim():
    assert_equal( tagged_dim(['tag1', 'dim']) , TaggedDim(dim=Dim('dim'), tags=[Tag('tag1')]))
    assert_equal( tagged_dim('dim') , TaggedDim(dim=Dim('dim')))
    assert_equal( tagged_dim(':dim') , TaggedDim(dim=Dim('dim')))
    assert_equal( tagged_dim('tag1:') , TaggedDim(tags=[Tag('tag1')]))
    assert_equal( tagged_dim('tag1:dim') , TaggedDim(dim=Dim('dim'), tags=[Tag('tag1')]))
    assert_equal( tagged_dim('tag1:tag2:dim') , TaggedDim(dim=Dim('dim'), tags=[Tag('tag1'), Tag('tag2')]))

