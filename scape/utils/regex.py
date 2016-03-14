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

'''scape.utils.regex

Regular expressions for various cyber quantities (registry dimensions).

Currently only IPs, MAC addresses
'''
import re

IP = re.compile(r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b")
IPWHOLE=IP

IPOCTETS = re.compile(r"(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)")

IPALL = re.compile(r"((?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))")

IPONLY = re.compile(r"^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")

#MAC = re.compile(r"\w\w:\w\w:\w\w:\w\w:\w\w:\w\w")

MAC = re.compile(r"\b(?:[a-fA-F0-9][a-fA-F0-9][:-]){5}[a-fA-F0-9][a-fA-F0-9]\b")

MACONLY = re.compile(r"^(?:[a-fA-F0-9][a-fA-F0-9][:-]){5}[a-fA-F0-9][a-fA-F0-9]$")

MACPARTS = re.compile(r"\b([a-fA-F0-9][a-fA-F0-9])[:-]([a-fA-F0-9][a-fA-F0-9])[:-]([a-fA-F0-9][a-fA-F0-9])[:-]([a-fA-F0-9][a-fA-F0-9])[:-]([a-fA-F0-9][a-fA-F0-9])[:-]([a-fA-F0-9][a-fA-F0-9])\b")

def slash24(prefix):
    ipre = '\.'.join(prefix.split('.'))
    return re.compile('^{}\..*$'.format(ipre))
