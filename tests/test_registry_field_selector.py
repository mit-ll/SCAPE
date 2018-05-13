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

from scape.registry.field import Field
from scape.registry.tagged_dim import tagged_dim, TaggedDim
from scape.registry.utils import field_or_tagged_dim

def test_field_selector():
    assert_equal( field_or_tagged_dim(' @f '), Field('f'))
    assert_equal(field_or_tagged_dim(Field('f')), Field('f'))

    assert_equal( field_or_tagged_dim('tag:dim'), tagged_dim('tag:dim'))
    assert_equal( field_or_tagged_dim(['tag','dim']), tagged_dim('tag:dim'))

    assert_equal( field_or_tagged_dim(tagged_dim('tag:dim')), tagged_dim('tag:dim'))

@raises(ValueError)
def test_empty_field_selector():
    field_or_tagged_dim('')
