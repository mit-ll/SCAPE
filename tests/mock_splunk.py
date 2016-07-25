import os
import base64
import sys
import random
import string
import logging
import pprint
from collections import OrderedDict

if sys.version_info[:2] > (2, 7):
    # Python 3.X
    from unittest.mock import patch
    from urllib.parse import parse_qs, parse_qsl, urlparse, unquote_plus
elif sys.version_info[:2] == (2 , 7):
    # Python 2.7
    from mock import patch
    from urllib import unquote_plus 
    from urlparse import parse_qs, parse_qsl, urlparse
else:
    raise ImportError('cannot run on Python < 2.7')

import requests
from httmock import (
    all_requests, response, urlmatch, HTTMock
)

import scape.splunklite as slite

_log = logging.getLogger('mock_splunk')
_log.addHandler(logging.NullHandler())

HEADERS_BP = {
    'cache-control': 'no-store, no-cache, must-revalidate, max-age=0',
    'connection': 'Keep-Alive',
    'content-type': 'XXXXXX',
    'date': 'Wed, 11 May 2016 02:14:12 GMT',
    'expires': 'Thu, 26 Oct 1978 00:00:00 GMT',
    'server': 'Splunkd',
    'x-content-type-options': 'nosniff',
    'x-frame-options': 'SAMEORIGIN',
}

def clone_headers(kw=None,**kwargs):
    headers = HEADERS_BP.copy()
    if kw is not None:
        headers.update(kw)
    headers.update(kwargs)
    return headers

XML_MT = 'text/xml; charset=UTF-8'
JSON_MT = 'application/json; charset=UTF-8'

xml_header = lambda: clone_headers({'content-type': XML_MT})
json_header = lambda: clone_headers({'content-type': JSON_MT})

JOB_CREATE_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<response><sid>{sid}</sid></response>
'''

def job_create_xml(sid):
    return JOB_CREATE_XML.format(sid=sid)
        
JOB_ATTR_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xml" href="/static/atom.xsl"?>
<entry xmlns="http://www.w3.org/2005/Atom" xmlns:s="http://dev.splunk.com/ns/rest" xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
  <content type="text/xml">
    <s:dict>
{keys}
    </s:dict>
  </content>
</entry>
'''

def job_attr_xml(kw=None, **kwargs):
    args = kw.copy() if kw is not None else {}
    args.update(kwargs)
    keys = ''
    kstr = '      <s:key name="{}">{}</s:key>\n'
    for key,value in args.items():
        keys += kstr.format(key,value)
    return JOB_ATTR_XML.format(keys=keys)

XML_401 = '''<?xml version="1.0" encoding="UTF-8"?>
<response>
  <messages>
    <msg type="WARN">call not properly authenticated</msg>
  </messages>
</response>
'''
XML_400 = '''<?xml version="1.0" encoding="UTF-8"?>
<response>
  <messages>
    <msg type="FATAL">Unknown search command XXX</msg>
  </messages>
</response>
'''

JOB_CONTROL_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<response><messages><msg type='INFO'>{action}</msg></messages></response>
'''

def job_control_xml(kw=None, **kwargs):
    args = kw.copy() if kw is not None else {}
    args.update(kwargs)
    return JOB_CONTROL_XML.format(**args)

def randstr(N):
    return ''.join(random.choice(string.ascii_lowercase) for i in range(N))

class SplunkHost(dict):
    def __init__(self, host, port, username, password, protocol='https'):
        self['session_key'] = base64.b64encode(os.urandom(64)).decode()
        self['host'] = host
        self['port'] = port
        self['username'] = username
        self['password'] = password
        self['protocol'] = protocol
        self['url'] = "{0}://{1}:{2}".format(self['protocol'], self['host'],
                                             self['port'])
        _log.debug('Splunk host for test: \n{}'.format(pprint.pformat(self)))

        self.generate_results()

    def generate_results(self):
        hosts = ['host{}'.format(i) for i in range(10)]
        def randip():
            return '192.168.1.{}'.format(random.randrange(256))

        self.addc_results = [
            {'Source_Network_Address': randip(),
             'Source_Port': random.randrange(2**16),
             'host': random.choice(hosts)}
                for i in range(64)
        ]

    @classmethod
    def random(cls):
        kw = {
            'protocol': random.choice(['http','https']),
            'host': randstr(16),
            'port': random.randrange(2**16),
            'username': randstr(16),
            'password': randstr(24),
        }
        return cls(**kw)

    def connect_kw(self):
        return {k:self[k] for k in ['protocol','host','port','username',
                                    'password']}

    def service(self):
        @all_requests
        def session_key_200(url, request):
            content = '<response>\n  <sessionKey>{}</sessionKey>\n</response>\n'.format(self['session_key'])
            headers = clone_headers({
                'content-type': XML_MT,
            })
            return response(200, content, headers, None, 5, request)

        with HTTMock(session_key_200):
            service = slite.Service(**self.connect_kw())
            service.session_key # because it's lazy
        return service

    @urlmatch(path=r'/services/search/jobs$', method='POST')
    def job_create_200(self, url, request):
        content = job_create_xml(randstr(16))
        headers = clone_headers({
            'content-type': XML_MT,
        })
        return response(200, content, headers, None, 5, request)

    def job_create_with_id_200(self, sid):
        @urlmatch(path=r'/services/search/jobs$', method='POST')
        def job_create_200(url, request):
            content = job_create_xml(sid)
            headers = clone_headers({
                'content-type': XML_MT,
            })
            return response(200, content, headers, None, 5, request)
        return job_create_200
        

    @urlmatch(path=r'/services/search/jobs$', method='POST')
    def job_create_400(self, url, request):
        ''' 400: bad search command
        '''
        return response(400, XML_400, xml_header(), None, 5, request)

    @urlmatch(path=r'/services/search/jobs$', method='POST')
    def job_create_401(self, url, request):
        ''' 401: bad search command
        '''
        return response(401, XML_401, xml_header(), None, 5, request)

    @urlmatch(path=r'/services/search/jobs/[^/]*(?!/.*)$', method='GET')
    def job_attr_200(self, url, request):
        content = job_attr_xml({
            'isDone': 1,
            'dispatchState': 'FINISHED',
        })
        headers = clone_headers({
            'content-type': XML_MT,
        })
        return response(200, content, headers, None, 5, request)

    def job_attr_with_attrs_200(self, attrs):
        @urlmatch(path=r'/services/search/jobs/[^/]*(?!/.*)$', method='GET')
        def job_attr_200(url, request):
            content = job_attr_xml(attrs)
            headers = clone_headers({
                'content-type': XML_MT,
            })
            return response(200, content, headers, None, 5, request)
        return job_attr_200

    def job_not_ready(self):
        return self.job_attr_with_attrs_200({
            'isDone': '0',
            'dispatchState': 'QUEUED',
        })
    def job_ready_not_done(self):
        return self.job_attr_with_attrs_200({
            'isDone': '0',
            'dispatchState': 'FINISHED',
        })
    def job_ready_and_done(self):
        return self.job_attr_with_attrs_200({
            'isDone': '1',
            'dispatchState': 'FINISHED',
        })

    @urlmatch(path=r'/services/search/jobs/(.*?)/results(?:_preview)?$',method='GET')
    def addc_results_200(self, url, request):
        params = OrderedDict(parse_qsl(url.query))
        offset = int(params.get('offset',0))
        count = int(params.get('count',100))
        if count == 0:
            count = len(self.addc_results)

        results = self.addc_results[offset:offset+count]

        content = {
            'messages': [],
            'results': results,
        }
        headers = clone_headers({
            'content-type': JSON_MT,
        })
        return response(200, content, headers, None, 5, request)

    def results_200(self, results):
        @urlmatch(path=r'/services/search/jobs/(.*?)/results(?:_preview)?$',
                  method='GET')
        def res200(url, request):
            if not hasattr(res200,'index'):
                res200.index = 0
            params = OrderedDict(parse_qsl(url.query))
            offset = int(params.get('offset',0))
            count = int(params.get('count',100))
            if count == 0:
                count = len(results)
            content = {
                'messages': [],
                'results': results[res200.index:res200.index+count],
            }
            res200.index += count
            headers = clone_headers({
                'content-type': JSON_MT,
            })
            return response(200, content, headers, None, 5, request)

        return res200
            
        

    @urlmatch(path=r'/services/search/jobs/(.*?)/control$',method='POST')
    def control_200(self, url, request):
        headers = clone_headers({
            'content-type': XML_MT,
        })
        params = OrderedDict(parse_qsl(request.body))
        return response(200, job_control_xml(action=params['action']),
                        headers, None, 5, request)

    
