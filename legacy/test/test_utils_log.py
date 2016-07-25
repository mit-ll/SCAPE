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
#!/usr/bin/env python

import unittest

import scape.utils

from mock import Mock

class UtilsLogTest(unittest.TestCase):
    def test_lines(self):
        self.assertEquals(
            scape.utils.lines('a','b'),
            '\na\nb'
        )
        self.assertEquals(
            scape.utils.lines('a','b',('c','d')),
            '\na\nb\nc d'
        )
        

#----------------------------------------------

def main():
    unittest.main()

if __name__ == '__main__':
    main()
