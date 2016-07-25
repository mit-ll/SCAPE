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
import gzip
import bz2

import scape.utils.file

class TestMakedirs(TestCase):
    temp_dir = ''
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp('test_utils_file')

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_makedirs(self):
        dirs = ['makedirs','new','dir']
        path = os.path.join(self.temp_dir,*dirs)
        root,newdirs = scape.utils.file.makedirs(path)

        self.assertTrue(os.path.exists(path))

        self.assertEquals(root, self.temp_dir)
        self.assertEquals(newdirs, dirs)

    def test_makedirs_rwx(self):
        dirs = ['makesdirs_rwx','new','dir']
        path = os.path.join(self.temp_dir,*dirs)
        root,newdirs = scape.utils.file.makedirs_rwx(path)

        self.assertTrue(os.path.exists(path))

        self.assertEquals(root, self.temp_dir)
        self.assertEquals(newdirs, dirs)

        for i in range(len(dirs)):
            p = os.path.join(self.temp_dir,*dirs[:i+1])
            self.assertTrue(os.stat(p).st_mode & scape.utils.file.RWX)

    def test_makedirs_rw(self):
        dirs = ['makesdirs_rw','new','dir']
        path = os.path.join(self.temp_dir,*dirs)
        root,newdirs = scape.utils.file.makedirs_rw(path)

        self.assertTrue(os.path.exists(path))

        self.assertEquals(root, self.temp_dir)
        self.assertEquals(newdirs, dirs)

        for i in range(len(dirs)):
            p = os.path.join(self.temp_dir,*dirs[:i+1])
            self.assertTrue(os.stat(p).st_mode & scape.utils.file.RWX)


def test_content():
    return '''test content'''
    
class TestZipOpen(TestCase):
    temp_dir = ''
    temp_path = ''
    temp_path_comp = {
        'gz': '',
        'bz2': '',
        }
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp('test_utils_file')

        self.temp_path = os.path.join(self.temp_dir,'temp.dat')
        with open(self.temp_path,'w') as wfp:
            wfp.write(test_content())

        gz_path = os.path.join(self.temp_dir,'temp.dat.gz')
        self.temp_path_comp['gz'] = gz_path
        with gzip.open(gz_path,'wb') as wfp:
            wfp.write(test_content())

        bz2_path = os.path.join(self.temp_dir,'temp.dat.bz2')
        self.temp_path_comp['bz2'] = bz2_path
        with bz2.BZ2File(bz2_path,'wb') as wfp:
            wfp.write(test_content())

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_zip_open(self):
        for path in self.temp_path_comp.values():
            with scape.utils.file.zip_open(path) as rfp:
                data = rfp.read()
            self.assertEquals(data,test_content())


if __name__=='__main__':
    unittest.main()
        
