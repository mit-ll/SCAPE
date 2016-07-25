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
from __future__ import absolute_import
import os
import sys
import argparse
import subprocess
import shlex
import xml.etree.cElementTree as cet
import cStringIO
from datetime import datetime,timedelta

import scape.utils
import scape.utils.log
_log = scape.utils.log.new_log('scape.utils.tshark')

_tags = set()
class Namespace(object):
    def __init__(self,node=None):
        if node is not None:
            self._type = node.tag
        else:
            self._type = None
        self._attributes = []
        self._dict = {}
    def __repr__(self):
        if self._type:
            name = self._dict.get('name')
            tstr = '{}:{}'.format(self._type,name) if name else self._type
            repr_str = '<{} with: {}>'.format(tstr,', '.join(self._attributes))
        else:
            repr_str = '<Null>'
        return repr_str
    def __getitem__(self,key):
        return self._dict[key]
    def __setitem__(self,key,value):
        self._dict[key] = value
    def __contains__(self,attr):
        return attr in self._attributes
    def __nonzero__(self):
        return self._type is not None
    def __getattr__(self,attr):
        if attr[0] != '_':
            return Namespace()
        else:
            return getattr(self,attr)


class Value(str):
    def __init__(self,*a,**kw):
        super(Value,self).__init__(*a,**kw)
        self._dict = {}
    def __getitem__(self,key):
        return self._dict[key]
    def __setitem__(self,key,value):
        self._dict[key] = value

class NList(list):
    def __getattr__(self,attr):
        spaces = filter(None,[getattr(s,attr) for s in self])
        return NList(spaces)

BP = '''
tshark -r {path} -T pdml
'''

def parse_pcap_whole(path):
    P = subprocess.Popen(BP.format(path=path),shell=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o,e = P.communicate()
    if P.returncode:
        raise IOError(e)
    rfp = cStringIO.StringIO(o)
    generator = cet.iterparse(rfp,events=('start','end'))

    event,root = next(generator)

    packets = []

    def add(parent,node):
        name = node.attrib['name'].split('.')[-1]
        new = Namespace(node)
        parent._attributes.append(name)
        setattr(parent,name,new)
        new._dict.update(node.attrib)
        return new

    for event,node in generator:
        tag = node.tag
        if (event,tag) == ('start','packet'):
            packet_node = node
            packet = Namespace(node)

        elif (event,tag) == ('start','proto'):
            protocol = add(packet,node)
            fstack = [protocol]

        elif (event,tag) == ('start','field'):
            parent = fstack[-1]
            field = add(parent,node)
            fstack.append(field)

        elif (event,tag) == ('end','field'):
            field = fstack.pop()
            if not field._attributes:
                name = field['name'].split('.')[-1]
                value = Value(field['show'])
                for k,v in field._dict.items():
                    value[k] = v 
                setattr(fstack[-1],name,value)

        elif (event,tag) == ('end','packet'):
            packets.append(packet)
            root.clear()

    #assert all([p._type == 'packet' for p in packets])
    return NList(packets)

def tshark_command(path,fields):
    command = ['tshark','-r',os.path.abspath(path),'-T','fields']
    for f in fields:
        command.extend(['-e',f])
    command = ' '.join(command)
    return shlex.split(command)

def parse_pcap(path,fields,row_callback):
    ''' parse a PCAP/ERF file using tshark
    
    Usage:
    >>> unique_ips = set()
    >>> def get_ips(row):
    ...     src_ip = row['ip.src']
    ...     dst_ip = row['ip.dst']
    ...     unique_ips.add(src_ip)
    ...     unique_ips.add(dst_ip)
    ...
    >>> parse_pcap('/path/to/my_erf_file.erf',['ip.src','ip.dst'],get_ips)
    >>> print scape.utils.sort_ips(unique_ips)
    '''
    command = tshark_command(path,fields)
    pipe = subprocess.Popen(
        command,
        bufsize=1,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True,
        )

    def build_row(line):
        row = {fields[i]:v.strip() for i,v in enumerate(line.split('\t'))}
        return row
    
    while pipe.poll() is None:
        line = pipe.stdout.readline()
        if line:
            row_callback(build_row(line))
    lines = pipe.stdout.readlines()
    if lines:
        for line in lines:
            row_callback(build_row(line))
    return pipe.returncode

