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

# Tag ##################################################################

def test_tag_repr():
    r = Tag('tag')
    assert_equal( r , eval(repr(r)))

def test_tag_html():
    Tag('g')._repr_html_()

@raises(ValueError)
def test_bad_arg():
    Tag(1)

def test_tag():
    assert_equal( tag('t') , Tag('t'))

