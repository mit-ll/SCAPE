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
import logging
import time
import random
import atexit
import traceback
import subprocess

import psutil

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol

import scape.config
from scape.utils import lines

from proxy import AccumuloProxy

from scape_accumulo.exceptions import ScapeAccumuloError

_log = logging.getLogger('scape_accumulo.connection')
_log.addHandler(logging.NullHandler())


def open_ip_ports():
    C = psutil.net_connections('all')
    ip_ports = reduce(lambda x,y:x|y,[set([x.laddr,x.raddr]) for x in C],set())
    return { a for a in ip_ports if (type(a) is tuple) and len(a)==2 }

def open_ports():
    ip_ports = open_ip_ports()
    return {a[-1] for a in ip_ports}

def available_ports():
    taken = open_ports()
    potential = set(range(42424,42455))
    available = potential - taken
    return available

def get_proxy_properties_dir():
    home = scape.config.config.user_home
    proxy_dir = os.path.join(home,'proxies')
    if not os.path.exists(proxy_dir):
        os.makedirs(proxy_dir)
    return proxy_dir

def get_proxy_properties(port):
    proxy_dir = get_proxy_properties_dir()
    properties_path = os.path.join(
        proxy_dir,'proxy_{}.properties'.format(port)
        )

    if not os.path.exists(properties_path):
        with open(properties_path,'wb') as wfp:
            aconfig = scape.config.config['accumulo']
            instance = aconfig['instance']
            zookeepers = aconfig['zookeepers']
            mock = False
            mini = False
            wfp.write(
                _PROXY_PROPERTIES.format(
                    mock=str(mock).lower(),
                    mini=str(mini).lower(),
                    port=port,
                    instance=instance,
                    zookeepers=zookeepers,
                    )
                )

    return properties_path

_PROXY_PROPERTIES = '''
useMockInstance={mock}
useMiniAccumulo={mini}
protocolFactory=org.apache.thrift.protocol.TCompactProtocol$Factory
tokenClass=org.apache.accumulo.core.client.security.tokens.PasswordToken
port={port}
maxFrameSize=16M

instance={instance}
zookeepers={zookeepers}
'''

class Connection(object):
    ''' doc for Connection '''
    host = scape.config.config['accumulo']['proxy']['host']
    port = scape.config.config['accumulo']['proxy']['port']
    user = scape.config.config['accumulo']['user']
    password = scape.config.config['accumulo']['password']

    def __init__(self,host=host, port=None, user=user, password=password):
        self.host = host
        self.port = int(port) if port is not None else port
        self.user = user
        self.password = password

    @property
    def settings(self):
        return {'host': self.host, 'port': self.port,
                'user': self.user, 'password': self.password}

    def __del__(self):
        _log.debug(lines(('__del__',self.__class__)))
        self.close()

    def close(self):
        if self.connected:
            T = self.transport
            self._login = None
            self._client = None
            self._protocol = None
            self._transport = None
            try:
                T.close()
            except:
                _log.error(lines(
                        'Could not close transport:',
                        traceback.format_exc(),
                        ))

    def reset(self):
        self.close()

    @property
    def connected(self):
        if self._transport:
            return self._transport.isOpen()
        return False

    def reset_proxy(self):
        Connection._proxy = None
        
    _proxy = None
    @property
    def proxy(self):
        class Proxy(object):
            pipe = None
            port = None
            def __del__(self):
                if self.pipe:
                    try:
                        self.pipe.terminate()
                    except OSError:
                        pass
            pass
        if Connection._proxy is None:
            available = list(available_ports())
            random.shuffle(available)
            for port in available:
                properties_path = get_proxy_properties(port)
                accumulo = os.path.join(
                    os.environ['ACCUMULO_HOME'],'bin','accumulo',
                    )

                pipe = subprocess.Popen(
                    [accumulo,'proxy','-p',properties_path],
                    stderr=subprocess.STDOUT,stdout=subprocess.PIPE,
                    )

                time.sleep(1)
                
                check = subprocess.Popen(
                    ['jstack','-l',str(pipe.pid)],
                    stderr=subprocess.PIPE,stdout=subprocess.PIPE,
                    )

                out,err = check.communicate()
                if check.returncode:
                    _log.error(lines(err))
                    try:
                        pipe.terminate()
                    except OSError:
                        pass
                    continue

                @atexit.register
                def kill():
                    try:
                        _log.debug('Terminating proxy')
                        pipe.terminate()
                    except OSError:
                        pass
                

                proxy = Proxy()
                proxy.pipe = pipe
                proxy.port = port
                Connection._proxy = proxy
                break
            
        return Connection._proxy

    _transport = None
    @property
    def transport(self):
        if self._transport is None:
            if self.port is None:
                self.port = self.proxy.port
            self._transport = TTransport.TFramedTransport(
                TSocket.TSocket(self.host, self.port)
            )
        return self._transport
    @transport.setter
    def transport(self,transport):
        self._transport = transport

    _protocol = None
    @property
    def protocol(self):
        if self._protocol is None:
            self._protocol = TCompactProtocol.TCompactProtocol(self.transport)
        return self._protocol
    @protocol.setter
    def protocol(self,protocol):
        self._protocol = protocol

    _client = None
    @property
    def client(self):
        if self._client is None:
            self._client = AccumuloProxy.Client(self.protocol)
        return self._client
    @client.setter
    def client(self,client):
        self._client = client

    _login = None
    @property
    def login(self):
        if self._login is None:
            try:
                self.transport.open()
            except:
                self.reset()
                self.transport.open()
            self._login = self.client.login(
                self.user, {'password': self.password},
            )
        return self._login
    @login.setter
    def login(self,login):
        self._login = login

    @property
    def tables(self):
        return self.client.listTables(self.login)


