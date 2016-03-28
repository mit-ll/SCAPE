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

import os
import tempfile
import shutil
import unittest
from unittest import TestCase
import csv

import scape.utils.csv

class TestCSVRow(TestCase):
    temp_dir = ''
    path = {
        'header': '',
        'noheader': '',
        }
    columns = ['a','b','c']
    rows = [['0','1','2'],['3','4','5']]
    drows = None

    def setUp(self):
        self.drows = [{c:r[i] for i,c in enumerate(self.columns)}
                      for r in self.rows]
        
        self.temp_dir = tempfile.mkdtemp('test_utils_file')

        self.path['header'] = os.path.join(self.temp_dir,'test.csv')
        with open(self.path['header'],'wb') as wfp:
            writer = csv.DictWriter(wfp,self.columns)
            writer.writeheader()
            writer.writerows(self.drows)

        self.path['noheader'] = os.path.join(self.temp_dir,
                                                  'test.nohead.csv')
        with open(self.path['noheader'],'wb') as wfp:
            writer = csv.writer(wfp)
            writer.writerows(self.rows)

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_header(self):
        self.assertEqual(
            list(scape.utils.csv.csv_rows(self.path['header'])),
            self.drows
        )

    def test_no_header(self):
        self.assertEqual(
            list(scape.utils.csv.csv_rows(self.path['noheader'],
                                          columns=self.columns,
                                          header=False)),
            self.drows
        )


if __name__=='__main__':
    unittest.main()
        
