import os
import base64
import sys
import random
import string
import logging
import pprint

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

import unittest
import scape.splunklite as slite

_log = logging.getLogger('test_splunklite')
_log.addHandler(logging.NullHandler())


XML_MT = 'text/xml; charset=UTF-8'
JSON_MT = 'application/json; charset=UTF-8'

HEADER_BP = {
    'cache-control': 'no-store, no-cache, must-revalidate, max-age=0',
    'connection': 'Keep-Alive',
    'content-type': 'XXXXXX',
    'date': 'Wed, 11 May 2016 02:14:12 GMT',
    'expires': 'Thu, 26 Oct 1978 00:00:00 GMT',
    'server': 'Splunkd',
    'x-content-type-options': 'nosniff',
    'x-frame-options': 'SAMEORIGIN',
}

class Url(object):
    '''A url object that can be compared with other url orbjects
    without regard to the vagaries of encoding, escaping, and ordering
    of parameters in query strings.'''

    def __init__(self, url):
        parts = urlparse(url)
        _query = frozenset(parse_qsl(parts.query))
        _path = unquote_plus(parts.path)
        parts = parts._replace(query=_query, path=_path)
        self.parts = parts

    def __eq__(self, other):
        return self.parts == other.parts

    def __hash__(self):
        return hash(self.parts)

@all_requests
def get_response_content_200(url, request):
    content = {'a':'b'}
    headers = HEADER_BP.copy()
    headers['content-type'] = JSON_MT
    return response(200, content, headers, None, 5, request)

@all_requests
def get_response_content_404(url, request):
    content = {'a':'b'}
    headers = HEADER_BP.copy()
    headers['content-type'] = JSON_MT
    return response(404, content, headers, None, 5, request)


class TestGetPostUnverified(unittest.TestCase):

    def test_post_unverified_200(self):
        with HTTMock(get_response_content_200):
            r = slite.post_unverified('http://localhost:8089')
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json(), {'a':'b'})

    def test_post_unverified_404(self):
        with HTTMock(get_response_content_404):
            r = slite.post_unverified('http://localhost:8089')
            self.assertEqual(r.status_code, 404)
            self.assertEqual(r.json(), {'a':'b'})

    def test_get_unverified_200(self):
        with HTTMock(get_response_content_200):
            r = slite.get_unverified('http://localhost:8089')
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json(), {'a':'b'})

    def test_get_unverified_404(self):
        with HTTMock(get_response_content_404):
            r = slite.get_unverified('http://localhost:8089')
            self.assertEqual(r.status_code, 404)
            self.assertEqual(r.json(), {'a':'b'})

class TestHttpTrait(unittest.TestCase):
    def setUp(self):
        class TestHttp(slite.HttpTrait):
            @property
            def url(self):
                return 'http://testurl.com/a/b/c'
            @property
            def header(self):
                return {'X': 'Y'}
            @property
            def params(self):
                return {'A':'B', 'C':'D'}
            @property
            def data(self):
                return {'W':'R', 'E':'T'}
        self.http = TestHttp()

    def test_http_get(self):
        with HTTMock(get_response_content_200):
            r = self.http.http_get()
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.request.method, 'GET')
            self.assertEqual(
                Url(r.url),
                Url('http://testurl.com/a/b/c?A=B&C=D')
            )

            r = self.http.http_get(l='m')
            self.assertEqual(
                Url(r.url),
                Url('http://testurl.com/a/b/c?A=B&C=D&l=m')
            )

    def test_http_post(self):
        with HTTMock(get_response_content_200):
            r = self.http.http_post()
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.request.method, 'POST')
            self.assertEqual(Url(r.url), Url('http://testurl.com/a/b/c'))
            self.assertEqual(
                Url('http://x?'+r.request.body), Url('http://x?W=R&E=T')
            )


def randstr(N):
    return ''.join(random.choice(string.ascii_lowercase) for i in range(N))

class RandomSplunkHost(dict):
    def __init__(self):
        self['session_key'] = base64.b64encode(os.urandom(64)).decode()
        self['protocol'] = random.choice(['http','https'])
        self['host'] = randstr(16)
        self['port'] = random.randrange(2**16)
        self['username'] = randstr(16)
        self['password'] = randstr(24)
        self['url'] = "{0}://{1}:{2}".format(self['protocol'], self['host'],
                                             self['port'])
        _log.debug('Random host for test: \n{}'.format(pprint.pformat(self)))

    def connect_kw(self):
        return {k:self[k] for k in ['protocol','host','port','username',
                                    'password']}

    def service(self):
        @all_requests
        def session_key_200(url, request):
            content = '<response>\n  <sessionKey>{}</sessionKey>\n</response>\n'.format(self['session_key'])
            headers = HEADER_BP.copy()
            headers['content-type'] = XML_MT
            return response(200, content, headers, None, 5, request)

        with HTTMock(session_key_200):
            service = slite.Service(**self.connect_kw())
            service.session_key # because it's lazy
        return service
        
class TestSplunkLiteService(unittest.TestCase):
    def setUp(self):
        self.host = RandomSplunkHost()

    def test_service_url(self):
        s = slite.Service(**self.host.connect_kw())
        self.assertEqual(s.url, self.host['url'])

    def test_session_key(self):
        @all_requests
        def session_key_200(url, request):
            content = '<response>\n  <sessionKey>{}</sessionKey>\n</response>\n'.format(self.host['session_key'])
            headers = HEADER_BP.copy()
            headers['content-type'] = XML_MT
            return response(200, content, headers, None, 5, request)

        @all_requests
        def session_key_404(url, request):
            return response(404, 'no content', {}, 'no reason', 5, request)

        with HTTMock(session_key_200):
            s = slite.Service(**self.host.connect_kw())
            self.assertEqual(s.session_key, self.host['session_key'])

        with HTTMock(session_key_404):
            s = slite.Service(**self.host.connect_kw())
            with self.assertRaises(slite.SplunkSessionKeyError):
                s.session_key

class TestSplunkLiteJobs(unittest.TestCase):
    def setUp(self):
        self.host = RandomSplunkHost()
        self.jobs_url = '{}/services/search/jobs'.format(self.host['url'])
        self.jobs_header = {
            'Authorization': 'Splunk {}'.format(self.host['session_key']),
        }

        self.service = self.host.service()

    def test_jobs_url(self):
        jobs = self.service.jobs
        self.assertEqual(jobs.url, self.jobs_url)

    def test_jobs_header(self):
        jobs = self.service.jobs
        self.assertEqual(jobs.header, self.jobs_header)
             
        
JOB_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xml" href="/static/atom.xsl"?>
<entry xmlns="http://www.w3.org/2005/Atom" xmlns:s="http://dev.splunk.com/ns/rest" xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
  <content type="text/xml">
    <s:dict>
{keys}
    </s:dict>
  </content>
</entry>
'''
def job_xml(**args):
    keys = ''
    kstr = '      <s:key name="{}">{}</s:key>\n'
    for key,value in args.items():
        keys += kstr.format(key,value)

class TestSplunkLiteJob(unittest.TestCase):
    def setUp(self):
        self.host = RandomSplunkHost()
        self.service = self.host.service()
        self.jobs = self.service.jobs

    def test_job_create_401(self):
        ''' 401: authorization failure
        '''
        pass

    def test_job_create_200(self):
        pass

#-------------------------------------------------------------------------------
# from httmock import all_requests, response, HTTMock
# import requests

# @all_requests
# def response_content(url, request):
#     headers = {'content-type': 'application/json',
#                'Set-Cookie': 'foo=bar;'}
#     content = {'message': 'API rate limit exceeded'}
#     return response(403, content, headers, None, 5, request)

# with HTTMock(response_content):
#     r = requests.get('https://api.github.com/users/whatever')

# print r.json().get('message')
# print r.cookies['foo']
#-------------------------------------------------------------------------------
