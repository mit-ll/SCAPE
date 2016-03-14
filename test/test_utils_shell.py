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
import gzip
import bz2

import scape.utils.shell as shell

class TestShell(TestCase):
    temp_dir = ''
    files = {'files':['file0','file1'],'dirs':['dir0']}
    ls_set = None
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp('test_utils_file')

        self.ls_set = set()
        
        for fname in self.files['files']:
            self.ls_set.add(fname)
            with open(os.path.join(self.temp_dir,fname),'wb'):
                pass

        for dname in self.files['dirs']:
            self.ls_set.add(dname)
            dpath = os.path.join(self.temp_dir,dname)
            os.makedirs(dpath)

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_sh(self):
        ls_command = 'ls {}'.format(self.temp_dir)
        out,err,pipe = shell.sh(ls_command)
        self.assertEqual(
            self.ls_set, {s.strip() for s in out.splitlines()}
        )

    def test_shwatch(self):
        ls_command = 'ls {}'.format(self.temp_dir)
        rc, lines = shell.shwatch(ls_command)
        self.assertEqual(
            self.ls_set, {l.strip() for l in lines}
        )
        
    def test_shiter(self):
        ls_command = 'ls {}'.format(self.temp_dir)
        names = set()

        for line in shell.shiter(ls_command):
            names.add(line.strip())

        self.assertEqual(
            self.ls_set, names
        )
        


if __name__=='__main__':
    unittest.main()
        
