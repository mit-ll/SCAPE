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

import scape.utils.data as data

class TestDataFuncs(TestCase):
    def test_sortd(self):
        self.assertEquals(
            data.sortd({'a': 5, 'b': 4, 'c': 3}),
            [('c', 3), ('b', 4), ('a', 5)]
        )

    def test_ndigits(self):
        self.assertEquals(data.ndigits(9999), 4)
        self.assertEquals(data.ndigits(10000), 5)
        self.assertEquals(data.ndigits(0), 1)
        self.assertEquals(data.ndigits(-9999), 5)
        self.assertEquals(data.ndigits(-10000), 6)

    def test_nzf(self):
        self.assertEquals(data.nzf(9999), 4)
        self.assertEquals(data.nzf(10000), 5)
        self.assertEquals(data.nzf(0), 1)
        self.assertEquals(data.nzf(-9999), 5)
        self.assertEquals(data.nzf(-10000), 6)

    def test_zf(self):
        self.assertEquals(data.zf(9),'09')
        self.assertEquals(data.zf(10),'10')
        self.assertEquals(data.zf(99),'99')
        self.assertEquals(data.zf(100),'100')
        self.assertEquals(data.zf(2345),'2345')

    def test_ZF(self):
        z = data.ZF(10000)
        self.assertEquals(z(9),'00009')
        self.assertEquals(z(10),'00010')
        self.assertEquals(z(99),'00099')
        self.assertEquals(z(100),'00100')
        self.assertEquals(z(2345),'02345')

        z = data.ZF(-10000)
        self.assertEquals(z(-9),'-00009')
        self.assertEquals(z(-10),'-00010')
        self.assertEquals(z(-99),'-00099')
        self.assertEquals(z(-100),'-00100')
        self.assertEquals(z(-2345),'-02345')
        
    def test_enumerate_reverse(self):
        self.assertEquals(
            list(data.enumerate_reverse(['a','b','c'])),
            [(2, 'c'), (1, 'b'), (0, 'a')]
        )
            

if __name__=='__main__':
    unittest.main()
        
