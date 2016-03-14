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
import sys
import struct
import collections

import scapy.layers.all

from scape.utils import lines
import scape.utils.log

_log = scape.utils.log.new_log('scape.utils.erf')

class ErfIterator(collections.Iterator):
    ''' Use scapy to provide an iterator, returning individual packet
    dictionaries, for ERF files

    REVISION HISTORY:
    01 Aug 2014  -  Alexia Schulz typed in from stackoverflow/questions/10672215

    04 Aug 2014  -  David O'Gwynn refactored (using http://stackoverflow.com/a/16761293) and integrated into SCAPE

    Usage:
    >>> unique_ips = set()
    >>> with ErfIterator('/path/to/my_erf_file.erf') as iterator:
    ...     for packet in iterator:
    ...         src_ip = packet['pkt']['IP'].src
    ...         dst_ip = packet['pkt']['IP'].dst
    ...         unique_ips.add(src_ip)
    ...         unique_ips.add(dst_ip)
    >>> print scape.utils.sort_ips(unique_ips)
    >>>
    >>> unique_ips = set()
    >>> def get_ips(packet):
    ...     src_ip = packet['pkt']['IP'].src
    ...     dst_ip = packet['pkt']['IP'].dst
    ...     unique_ips.add(src_ip)
    ...     unique_ips.add(dst_ip)
    ...
    >>> parse_erf('/path/to/my_erf_file.erf',get_ips)
    >>> print scape.utils.sort_ips(unique_ips)
    '''
    def __init__(self,path):
        self.path = path
        self._fp = None
        self.generator = self._generator()

    def _close(self):
        if self._fp is not None:
            if not self._fp.closed:
                self._fp.close()

    def __del__(self):
        _log.info(lines(('__del__',self)))
        self._close()

    def __enter__(self):
        return self
    def __exit__(self,exc_type,exc_value,tb):
        _log.info(lines(('__exit__',self)))
        self._close()

    def __next__(self):
        for record in self.generator:
            return record
        raise StopIteration

    def _generator(self):
        self._fp = open(self.path,'rb')
        header = self._fp.read(16)
        index = 0
        while header:
            if index%10000 == 0:
                _log.info(lines(('  packet:',index)))
            index += 1
            R = {}
            R['ts'] = struct.unpack('<Q',header[:8])[0]
            R.update( list(zip( ('type',   # ERF record type
                            'flags',  # Raw flags bit field
                            'rlen',   # Length of entire record
                            'lctr',   # Interstitial loss counter
                            'wlen'),  # Length of packet on wire
                           struct.unpack('>BBHHH',header[8:]) )) )
            R['iface']  = R['flags'] & 0x03
            R['rx_err'] = R['flags'] & 0x10 != 0

            #- Check if ERF Extension Header present. Each Extension
            #- Header is 8 bytes
            if R['type'] & 0x80:
                ext_header = self._fp.read(8)
                R.update(
                    list(zip( ('ext_hdr_signature',    # 1 byte
                          'ext_hdr_payload_hash', # 3 bytes
                          'ext_hdr_filter_color', # 1 byte
                          'ext_hdr_flow_hash',    # 3 bytes
                          ),
                         struct.unpack('>B3sB3s', ext_header)
                         ))
                    )
                R['pkt'] = self._fp.read( R['rlen'] - 24 )
            else:
                R['pkt'] = self._fp.read( R['rlen'] - 16 )

            if R['type'] & 0x02:
                # ERF Ethernet has an extra two bytes of pad between
                # ERF header and beginning of MAC header so that
                # IP-layer data are DWORD aligned.  From memory, none
                # of the other types have pad.
                R['pkt'] = R['pkt'][2:]
                R['pkt'] = scapy.layers.all.Ether( R['pkt'] )

            yield R

            header = self._fp.read(16)
            
def parse_erf(path,packet_callback):                
    with ErfIterator(path) as I:
        for p in I:
            packet_callback(p)
