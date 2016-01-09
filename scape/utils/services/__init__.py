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
'''scape.utils.services

TCP/IP port-to-service lookup functionality. Given a port, map it to
well-known applications that use that port.

'''

import os
import csv

__all__ = ['Port2Service']

def _csv_path():
    return os.path.join(os.path.abspath(os.path.split(__file__)[0]),
                        'service-names-port-numbers.csv')

def _get_rows(path):
    with open(path) as rfp:
        cols = next(csv.reader(rfp))
        rows = list(csv.DictReader(rfp,cols))
    return rows

def _get_port_lut(rows):
    lut = {}
    for r in rows:
        sname = r['Service Name'] or None
        desc = r['Description'] or None
        tp = r['Transport Protocol'] or None
        ports = r['Port Number']
        if ports:
            if '-' in ports:
                p0,p1 = [int(v) for v in ports.split('-')]
                ports = range(p0,p1+1)
            else:
                ports = [int(ports)]
        else:
            ports = [None]
        for p in ports:
            plist = lut.setdefault(p,[])
            plist.append((sname,desc,tp))
    return lut

class ServiceName(str):
    protocols = None

class Port2Service(dict):
    '''TCP/UDP port number to service name lookup table

    If simplified is True (default), then lookups on port numbers
    return a tuple of service names, each of which is a str object
    subclass with an additional protocol attribute. The protocol
    attribute is a dictionary mapping protocol to a tuple of
    descriptionsa

    If simplified is False, then port lookups return the list of
    (name, description, protocol) tuples associated with that port

    >>> p2s = scape.utils.services.Port2Service()
    >>> p2s[22]
    ('ssh',)
    >>> p2s['22']
    ('ssh',)
    >>> names = p2s[22]
    >>> names[0].protocols
    {'sctp': ['SSH'],
     'tcp': ['The Secure Shell (SSH) Protocol'],
     'udp': ['The Secure Shell (SSH) Protocol']}
    >>>
    >>> p2s = scape.utils.services.Port2Service(simplified=False)
    >>> p2s[22]
    [('ssh', 'The Secure Shell (SSH) Protocol', 'tcp'),
     ('ssh', 'The Secure Shell (SSH) Protocol', 'udp'),
     ('ssh', 'SSH', 'sctp')]
    >>>
    
    '''
    def __init__(self,simplified=True):
        super(Port2Service,self).__init__()
        self.simplified = simplified
        rows = _get_rows(_csv_path())
        lut = _get_port_lut(rows)
        self.update(lut)
    def __contains__(self,port):
        if not port:
            port = None
        else:
            port = int(port)
        return dict.__contains__(self,port)
    def __getitem__(self,port):
        if not port:
            port = None
        else:
            port = int(port)
        services = dict.__getitem__(self,port)
        if self.simplified:
            sdict = {name:set() for name,_,_ in services}
            for name,desc,proto in services:
                sdict[name].add((desc,proto))
            snames = []
            for name,infoset in sdict.items():
                s = ServiceName(name or '')
                s.protocols = {proto:[] for desc,proto in infoset}
                for desc,proto in infoset:
                    s.protocols[proto].append(desc)
                snames.append(s)
            return tuple(snames)
        else:
            return services
    def get(self,port,default=None):
        if port in self:
            return self[port]
        return default
