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

#! /usr/bin/env python2.7

import os
import sys
import pprint
from subprocess import Popen,PIPE

from scape.utils import sh

def hadoop(*args):
    command = "{hadoop}/bin/hadoop fs".format(
        hadoop = os.environ['HADOOP_HOME']
        )
    command = ' '.join([command]+['-'+args[0]]+list(args)[1:])
    return command

def ls(path):
    command = hadoop('ls',path)
    return sh(command)

def put(local_path,hdfs_path):
    # Save content at hdfs path using put method
    #
    command = hadoop('copyFromLocal',local_path,hdfs_path)
    out,err,pipe = sh(command)
    return out,err,pipe

def putData(hdfs_path,content):
    command = hadoop('put','-',hdfs_path)
    return sh(command,content)
