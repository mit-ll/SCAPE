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

''' scape.utils.ip

Utilities for dealing with IP addresses
'''
import socket
import struct
import random

from scape.utils.log import new_log
from scape.utils import regex

__all__ = ['is_ip','ip2num','num2ip','sort_ips','random_ip']

_log = new_log('scape.utils.ip')

def is_ip(ip):
    ''' Is this string an IP? '''
    return bool(regex.IPONLY.search(ip))

def ip2num(ip):
    ''' Convert string IP (dotted quad) to INET integer representation
    '''
    try:
        return struct.unpack('!I',socket.inet_aton(ip))[0]
    except socket.error:
        return 0

def num2ip(n):
    ''' Convert IP as INET integer representation to string (dotted quad)
    '''
    return socket.inet_ntoa(struct.pack('!I',n))

def sort_ips(ips):
    ''' Sort sequence of IPs 
    '''
    return sorted(ips,key=ip2num)

def random_ip():
    return num2ip(random.randint(0,2**32-1))
