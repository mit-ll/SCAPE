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
import unittest
from unittest import TestCase
import time
from datetime import date, datetime, timedelta

from acqua import (
    QueryException, JavaError
)
from acqua.operators import (
    PythonSource, Buffer
)

from scape.utils import ( memoized_property )
from scape.registry.exceptions import (
    ScapeTimeError
)
# from scape.registry.operators import (
#     pool, hist, distinct, uniq, limit, agg
# )

class TestOperators(TestCase):
    @memoized_property
    def registry(self):
        import test_registry_registry
        return test_registry_registry.registry_with_data()

    @memoized_property
    def Q(self):
        return self.registry.selection.question
        

    # def test_pool_consumer_timeout(self):
    #     I = self.Q|pool(size=1, consumer_timeout=1)
    #     with self.assertRaises(JavaError):
    #         next(I)
    #         time.sleep(1.01)
    #         next(I)

    # def test_pool_producer_timeout(self):
    #     def gen():
    #         for i in range(10):
    #             time.sleep(1.1)
    #             yield {'a':str(i),'b':str(i+1)}

    #     I = PythonSource(gen())|Buffer(size=1, source_timeout=1,
    #                                    consumer_timeout=10)
    #     with self.assertRaises(JavaError):
    #         next(I)

    # def test_histogram(self):
    #     self.Q(start='-1h')|hist('minute')

if __name__=='__main__':
    unittest.main()
    


