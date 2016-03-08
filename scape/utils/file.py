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

''' scape.utils.file

Utilities for dealing with files
'''
from __future__ import absolute_import
import os
import stat
import re
import gzip
import bz2
import pprint

from scape.utils.log import new_log

_log = new_log('scape.utils.file')

__all__ = ['RWX', 'RW', 'rwx', 'rw', 'makedirs', 'makedirs_rwx',
           'makedirs_rw', 'zip_open']

RWX = (
    stat.S_IRUSR|stat.S_IRGRP|stat.S_IROTH|
    stat.S_IWUSR|stat.S_IWGRP|stat.S_IWOTH|
    stat.S_IXUSR|stat.S_IXGRP|stat.S_IXOTH
    )

RW = (
    stat.S_IRUSR|stat.S_IRGRP|stat.S_IROTH|
    stat.S_IWUSR|stat.S_IWGRP|stat.S_IWOTH
    )

def rwx(path):
    ''' chmod path to a+rwx '''
    os.chmod(path,RWX)

def rw(path):
    ''' chmod path to a+rw '''
    os.chmod(path,RW)

def makedirs(directory):
    '''Create leaf directory and all necessary parent
    directories. Returns the extant root directory and a list of all
    created directories

    >>> makedirs('/existing/directory/path/to/new/leaf/dir')
    ('/existing/directory', ['path','to','new','leaf','dir'])

    '''
    newdirs = []
    root = directory
    while 1:
        if os.path.exists(root) or not root:
            break
        root,n = os.path.split(root)
        newdirs.append(n)
    os.makedirs(directory)
    return root,list(reversed(newdirs))

def makedirs_rwx(directory):
    '''Create directory (see makedirs above), chmod all newly-created
    directories to be a+rwx

    '''
    orig_root,newdirs = makedirs(directory)
    root = orig_root
    for dname in newdirs:
        rwx(os.path.join(root,dname))
        root = os.path.join(root,dname)
    return orig_root,newdirs

makedirs_rw = makedirs_rwx

tarfile_re = re.compile("\.tar(\.\w+)$")
def zip_open(path,mode='rb'):
    '''Given a path to a (possibly gzip- or bzip2-compressed) file, return
    the correct file opener. Warns if path ends in ".tar.*" or ".zip".

    Selects the correct decompression based on path.endswith
    (i.e. extension of the file).

    '''
    if tarfile_re.search(path):
        _log.warn("Using zip_open on TAR file: {}".format(path))
    if path.endswith('.zip'):
        _log.warn('Using zip_open on ZIP file: {}'.format(path))

    if path.endswith('.gz'):
        fp = gzip.open(path,mode)
    elif path.endswith('.bz2'):
        fp = bz2.BZ2File(path,mode)
    else:
        fp = open(path,mode)
    return fp

