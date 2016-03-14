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

import unittest
from unittest import TestCase

from scape.utils import ( memoized_property )
import scape.registry.utils as utils

class TestUtils(TestCase):
    @memoized_property
    def registry(self):
        import test_registry_registry
        return test_registry_registry.registry_with_data()

    def test_hashable_dict(self):
        hd = utils.HashableDict({'a':1})
        hd0 = hd
        hd1 = utils.HashableDict({'a':1})

        self.assertTrue(hd0 == hd1)
        self.assertTrue(hash(hd0) == hash(hd1))

        self.assertTrue({hd0: 5} == {hd1: 5})

        with self.assertRaises(AttributeError):
            hd['b'] = 2
        with self.assertRaises(AttributeError):
            hd.pop('a')
        with self.assertRaises(AttributeError):
            hd.popitem('a')
        with self.assertRaises(AttributeError):
            hd.clear()

    def test_tagged_dimension(self):
        td = map(utils.TaggedDimension,
                 ['d0','t0:d0',
                  't1:t0:d0','t0:t1:d0',
                  't2:t1:t0:d0','t1:t2:t0:d0',])
        self.assertEquals(str(td[0]), 'd0')
        self.assertEquals(td[0], 'd0')
        self.assertEquals(td[0].dim, 'd0')

        self.assertEquals(str(td[1]), 't0:d0')
        self.assertEquals(td[1], 't0:d0')
        self.assertEquals(td[1].dim, 'd0')
        self.assertEquals(td[1].tags, {'t0'})

        self.assertEquals(td[2], td[3])
        self.assertEquals(td[2].dim, td[3].dim)
        self.assertEquals(td[2].tags, td[3].tags)

        self.assertEquals(td[4], td[5])
        self.assertEquals(td[4].dim, td[5].dim)
        self.assertEquals(td[4].tags, td[5].tags)
        self.assertEquals(td[4], 't0:t1:t2:d0')
        self.assertEquals(td[5], 't0:t1:t2:d0')

        tdr = utils.TaggedDimension('t:date').resolve(self.registry.selection)
        self.assertEquals(tdr, 't0:t1:t2:t3:t4:datetime')
        tdr = utils.TaggedDimension('t:').resolve(self.registry.selection)
        self.assertEquals(tdr, 't0:t1:t2:t3:t4:')
            

if __name__=='__main__':
    unittest.main()
    
