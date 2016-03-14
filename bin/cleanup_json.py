#!/usr/bin/env python
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
import json
import argparse

import scape.utils

def get_args():
    parser = argparse.ArgumentParser(
        description=""" Given a list of paths to JSON files, read them in using
        scape.utils.json.literal_eval then write them back out as
        conformant JSON. This "cleans up" the JSON, removing dangling
        commas, etc. while preserving the key order in the JSON file.
        """
    )

    parser.add_argument('paths',nargs='+')

    args = parser.parse_args()
    return args

def cleanup_json(path):
    D = scape.utils.read_json(path)
    tmp = path+'.tmp'
    with open(tmp,'wb') as wfp:
        json.dump(D,wfp,indent=4)
    os.rename(tmp,path)

def main():
    args = get_args()
    for path in args.paths:
        cleanup_json(path)

if __name__=='__main__':
    main()
