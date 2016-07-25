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
import datetime
import gzip
import bz2

import scape.utils.json as json

class TestMergeDicts(TestCase):
    def test(self):
        self.assertEquals(
            json.merge_dicts({'a':[1]},{'a':[1,2]}),
            {'a': [1, 1, 2]},
        )
        self.assertEquals(
            json.merge_dicts({'a':(1,)},{'a':(1,2)}),
            {'a': (1, 1, 2)},
        )
        self.assertEquals(
            json.merge_dicts({'a':{1}},{'a':{1,2}}),
            {'a': {1, 2}},
        )
        self.assertEquals(
            json.merge_dicts({'a':{'b':4}},{'a':{'b':8}}),
            {'a': {'b':8}},
        )

class TestJsonDict(TestCase):
    def test(self):
        self.assertEquals(
            json.json_dict({'ts': datetime.datetime(2014,10,5,12)}),
            {'ts': '2014-10-05 12:00:00'},
        )
        self.assertEquals(
            json.json_dict({'a': [{1,2,3}, 'b', (1,2)]}),
            {'a': [[1,2,3], 'b', [1,2]]},
        )

def s_json():
    # standard JSON
    return '''
{ "a": [1,2,3],
  "b": 1.0,
  "c": "string"
}
'''

def ns_json():
    # non-standard JSON (i.e. more "forgiving" JSON format)
    return '''
{ 'a': [1,2,3,],
  'b': 1.0,
  'c': 'string',
}
'''

def ns_json_that_fails():
    # non-standard JSON that fails 
    return '''
{ 'a': true,,
}
    '''
    
class TestReadJson(TestCase):
    temp_dir = ''
    temp_json_path = ''
    temp_json_path_comp = {
        'gz': '',
        'bz2': '',
        }
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp('test_utils_json')

        self.temp_json_path = os.path.join(self.temp_dir,'temp.json')
        with open(self.temp_json_path,'w') as wfp:
            wfp.write(ns_json())

        gz_path = os.path.join(self.temp_dir,'temp.json.gz')
        self.temp_json_path_comp['gz'] = gz_path
        with gzip.open(gz_path,'wb') as wfp:
            wfp.write(ns_json())

        bz2_path = os.path.join(self.temp_dir,'temp.json.bz2')
        self.temp_json_path_comp['bz2'] = bz2_path
        with bz2.BZ2File(bz2_path,'wb') as wfp:
            wfp.write(ns_json())
    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_read_json_data(self):
        # standard JSON
        self.assertEquals(json.read_json_data(s_json()),
                          {'a': [1,2,3], 'b': 1.0, 'c': 'string'})

        # non-standard JSON
        self.assertEquals(json.read_json_data(ns_json()),
                          {'a': [1,2,3], 'b': 1.0, 'c': 'string'})

    def test_read_json(self):
        self.assertEquals(json.read_json(self.temp_json_path),
                          {'a': [1,2,3], 'b': 1.0, 'c': 'string'})

        for path in self.temp_json_path_comp.values():
            self.assertEquals(json.read_json(path),
                              {'a': [1,2,3], 'b': 1.0, 'c': 'string'})

    def test_read_json_data_fail(self):
        self.assertRaises(
            json.ScapeJsonReadError,
            json.read_json_data,ns_json_that_fails(),
        )

if __name__=='__main__':
    unittest.main()
        
