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

from scape.registry.dim import Dim, dim

# Dimension #############################################################

@raises(ValueError)
def test_dim_badarg():
    dim(1)

@raises(ValueError)
def test_dim_badarg2():
    Dim(1)

def test_dim_repr():
    d = dim('ip')
    assert_equal( d , eval(repr(d)))

def test_dim_html():
    d = dim('ip')
    d._repr_html_()

def test_parse_dims():
    assert_equal( dim(None) , None)
    assert_equal( dim('src') , Dim('src'))
    d = dim('src')
    assert_equal( dim(d) , d)

