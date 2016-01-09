"""
Copyright (2016) Massachusetts Institute of Technology.  Reproduction/Use 
of all or any part of this material shall acknowledge the MIT Lincoln 
Laboratory as the source under the sponsorship of the US Air Force 
Contract No. FA8721-05-C-0002.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
import os
import tempfile
import shutil
import unittest
from unittest import TestCase
import StringIO

import scape.utils.decorators as decorators

class TestMemoizationDecorators(TestCase):
    values = [1,2,3]
    values2 = [4,5,6]
    def test_memoized_property(self):
        class X(object):
            values = self.values
            times_called = 0
            @decorators.memoized_property
            def lazy_sum(self):
                self.times_called += 1
                return sum(self.values)

        x = X()

        self.assertEquals(x.times_called,0)

        self.assertEquals(x.lazy_sum,sum(self.values))

        self.assertEquals(x.times_called,1)

        x.lazy_sum

        self.assertEquals(x.times_called,1)
        
    def test_memoized_property_with_prop_none(self):
        class X(object):
            values = self.values
            times_called = 0
            _lazy_sum = None
            @decorators.memoized_property
            def lazy_sum(self):
                self.times_called += 1
                return sum(self.values)

        x = X()

        self.assertEquals(x.times_called,0)

        self.assertEquals(x.lazy_sum,sum(self.values))

        self.assertEquals(x.times_called,1)

        x.lazy_sum

        self.assertEquals(x.times_called,1)

    def test_watch_property(self):
        class X(object):
            values = self.values
            times_called = 0

            @decorators.watch_property('values')
            def lazy_sum(self):
                self.times_called += 1
                return sum(self.values)

        x = X()

        self.assertEquals(x.times_called,0)
        self.assertEquals(x.lazy_sum,sum(self.values))
        self.assertEquals(x.times_called,1)

        x.lazy_sum

        self.assertEquals(x.times_called,1)

        x.values = self.values2
        self.assertEquals(x.times_called,1)
        self.assertEquals(x.lazy_sum,sum(self.values2))
        self.assertEquals(x.times_called,2)

        x.lazy_sum

        self.assertEquals(x.times_called,2)

        x.lazy_sum_touch()
        self.assertEquals(x.times_called,2)
        self.assertEquals(x.lazy_sum,sum(self.values2))
        self.assertEquals(x.times_called,3)

        x.lazy_sum

        self.assertEquals(x.times_called,3)

    def test_singleton(self):
        @decorators.singleton
        def calc():
            calc.times_called += 1
            return reduce(lambda x,y:x+y,range(10))
        calc.times_called = 0

        self.assertEquals(calc(), 45)
        self.assertEquals(calc.times_called, 1)

        self.assertEquals(calc(), 45)
        self.assertEquals(calc.times_called, 1)

        
class TestReturnNulls(TestCase):
    def test_return_nulls(self):
        @decorators.remove_nulls('x')
        def test():
            return {'key0': 'x', 'key1': [], 'key2': 'a'}

        self.assertEquals(test(), {'key2':'a'})

if __name__=='__main__':
    unittest.main()
        
