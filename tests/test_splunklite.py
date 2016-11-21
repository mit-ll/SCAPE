# Copyright (2016) Massachusetts Institute of Technology.
# Reproduction/Use of all or any part of this material shall
# acknowledge the MIT Lincoln Laboratory as the source under the
# sponsorship of the US Air Force Contract No. FA8721-05-C-0002.

# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

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

from mock_splunk import (
    clone_headers, job_create_xml, job_attr_xml, job_control_xml, randstr,
    SplunkHost, XML_MT, JSON_MT, 
)

_log = logging.getLogger('test_splunklite')
_log.addHandler(logging.NullHandler())


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
    headers = clone_headers({
        'content-type': JSON_MT,
    })
    return response(200, content, headers, None, 5, request)

@all_requests
def get_response_content_404(url, request):
    content = {'a':'b'}
    headers = clone_headers({
        'content-type': JSON_MT,
    })
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


class TestSplunkLiteService(unittest.TestCase):
    def setUp(self):
        self.host = SplunkHost.random()

    def test_service_url(self):
        s = slite.Service(**self.host.connect_kw())
        self.assertEqual(s.url, self.host['url'])

    def test_session_key(self):
        @all_requests
        def session_key_200(url, request):
            content = '<response>\n  <sessionKey>{}</sessionKey>\n</response>\n'.format(self.host['session_key'])
            headers = clone_headers({
                'content-type': XML_MT,
            })
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
        self.host = SplunkHost.random()
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

TEST_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<entry xmlns="http://www.w3.org/2005/Atom" xmlns:s="http://dev.splunk.com/ns/rest" xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
  <content type="text/xml">
    <s:dict>
      <s:key name="a">b</s:key>
      <s:key name="c">d</s:key>
      <s:key name="e">5</s:key>
      <s:key name="f">
        <s:dict>
          <s:key name="a">b</s:key>
          <s:key name="c">d</s:key>
          <s:key name="e">5</s:key>
          <s:key name="g">
            <s:list>
              <s:item>1</s:item>
              <s:item>2</s:item>
              <s:item>a</s:item>
              <s:item>b</s:item>
            </s:list>
          </s:key>
        </s:dict>
      </s:key>
      <s:key name="g">
        <s:list>
          <s:item>1</s:item>
          <s:item>2</s:item>
          <s:item>a</s:item>
          <s:item>b</s:item>
        </s:list>
      </s:key>
    </s:dict>
  </content>
</entry>
'''
class TestResponseToDict(unittest.TestCase):
    def setUp(self):
        self.attr_dict = {
            'a': 'b',
            'c': 'd',
            'e': '5',
            'f': {'a':'b',
                  'c':'d',
                  'e':'5',
                  'g':['1','2','a','b']},
            'g':['1','2','a','b'],
        }

    def test_response_to_dict(self):
        ''' Reponse with TEST_XML
        '''
        self.assertEqual(
            self.attr_dict,
            slite.response_to_dict(response(200,TEST_XML)),
        )

    def test_degenerate_response_to_dict(self):
        ''' Response with Degenerate XML (empty string)
        '''
        self.assertEqual({}, slite.response_to_dict(response(200,'')))

    def test_empty_response_to_dict(self):
        ''' Response with Empty XML (no entry data)
        '''
        empty_xml = ('<?xml version="1.0" ?><entry></entry>')
        self.assertEqual({}, slite.response_to_dict(response(200,empty_xml)))

class TestSplunkLiteJob(unittest.TestCase):
    def setUp(self):
        self.host = SplunkHost.random()
        self.service = self.host.service()
        self.jobs = self.service.jobs

    def test_job_url(self):
        ''' Job URL
        '''
        sid = randstr(24)
        url = '{base}/services/search/jobs/{id}'.format(
            base=self.host['url'],id=sid
        )
        with HTTMock(self.host.job_create_with_id_200(sid)):
            job = self.jobs.create('*')
            self.assertEqual(url, job.url)

    def test_job_header(self):
        ''' Job header
        '''
        with HTTMock(self.host.job_create_200):
            job = self.jobs.create('*')
            self.assertEqual(job.header, self.jobs.header)

    def test_job_data(self):
        ''' Job data
        '''
        search_str = 'search index=main *'
        with HTTMock(self.host.job_create_200):
            job = self.jobs.create(search_str, a=1, b='asdf')
            self.assertEqual(
                {'search': search_str, 'a':1, 'b':'asdf' },
                job.data
            )

    def test_job_create_401(self):
        ''' 401: authorization failure
        '''
        with HTTMock(self.host.job_create_401):
            with self.assertRaises(slite.SplunkAuthenticationError):
                job = self.jobs.create('search index="main" *')
        
    def test_job_create_400(self):
        ''' 400: bad search
        '''
        with HTTMock(self.host.job_create_400):
            with self.assertRaises(slite.SplunkJobCreationError):
                job = self.jobs.create('BADSEARCH')
        

    def test_job_create_200(self):
        ''' 200: success
        '''
        sid = randstr(24)
        with HTTMock(self.host.job_create_with_id_200(sid)):
            job = self.jobs.create('*')
            self.assertEqual(job.id, sid)

    def test_refresh(self):
        with HTTMock(self.host.job_create_200):
            job = self.jobs.create('*')

        not_ready_attrs = {
            'isDone': '0',
            'dispatchState': 'QUEUED',
        }
        with HTTMock(self.host.job_attr_with_attrs_200(not_ready_attrs)):
            job.refresh()
            for key, value in not_ready_attrs.items():
                self.assertEqual(value, job[key])

        not_ready_attrs = {
            'isDone': '1',
            'dispatchState': 'ASDFASDF',
        }
        with HTTMock(self.host.job_attr_with_attrs_200(not_ready_attrs)):
            job.refresh()
            for key, value in not_ready_attrs.items():
                self.assertEqual(value, job[key])
        
    def test_is_ready(self):
        with HTTMock(self.host.job_create_200):
            job = self.jobs.create('*')

        with HTTMock(self.host.job_not_ready()):
            self.assertFalse(job.is_ready())

        with HTTMock(self.host.job_ready_not_done()):
            self.assertTrue(job.is_ready())
        
    def test_is_done(self):
        with HTTMock(self.host.job_create_200):
            job = self.jobs.create('*')

        with HTTMock(self.host.job_ready_not_done()):
            self.assertFalse(job.is_done())
        
        with HTTMock(self.host.job_ready_and_done()):
            self.assertTrue(job.is_done())
        

class TestResults(unittest.TestCase):
    def setUp(self):
        self.host = SplunkHost.random()
        self.service = self.host.service()
        self.jobs = self.service.jobs
        self.sid = randstr(24)
        with HTTMock(self.host.job_create_with_id_200(self.sid)):
            self.job = self.jobs.create('*')
            
    def test_results_count_1(self):
        results = [{'a':'b','c':'d'},{'e':'f','g':'h'}]
        with HTTMock(self.host.results_200(results)):
            r = self.job.results(count=1)
            _log.debug(r.params)
            self.assertEqual(1, r.params['count'])

    def test_results(self):
        results = [{'a':'b','c':'d'},{'e':'f','g':'h'}]
        with HTTMock(self.host.results_200(results)):
            self.assertEqual(results, list(self.job.results()))

    def test_results_iter(self):
        results = [{'a':'b','c':'d'},{'e':'f','g':'h'}]
        with HTTMock(self.host.results_200(results)):
            self.assertEqual(results[0], next(self.job.results()))
            

class TestControl(unittest.TestCase):
    def setUp(self):
        self.host = SplunkHost.random()
        self.service = self.host.service()
        self.jobs = self.service.jobs
        self.sid = randstr(24)
        with HTTMock(self.host.job_create_with_id_200(self.sid)):
            self.job = self.jobs.create('*')

    def test_pause(self):
        with HTTMock(self.host.control_200):
            self.assertTrue(self.job.control.pause())

    def test_unpause(self):
        with HTTMock(self.host.control_200):
            self.assertTrue(self.job.control.unpause())

    def test_finalize(self):
        with HTTMock(self.host.control_200):
            self.assertTrue(self.job.control.finalize())

    def test_cancel(self):
        with HTTMock(self.host.control_200):
            self.assertTrue(self.job.control.cancel())

    def test_touch(self):
        with HTTMock(self.host.control_200):
            self.assertTrue(self.job.control.touch())

    def test_setttl(self):
        with HTTMock(self.host.control_200):
            self.assertTrue(self.job.control.setttl(10))

    def test_setpriority(self):
        with HTTMock(self.host.control_200):
            self.assertTrue(self.job.control.setpriority(10))

    def test_enablepreview(self):
        with HTTMock(self.host.control_200):
            self.assertTrue(self.job.control.enablepreview())

    def test_disablepreview(self):
        with HTTMock(self.host.control_200):
            self.assertTrue(self.job.control.disablepreview())

    def test_save(self):
        with HTTMock(self.host.control_200):
            self.assertTrue(self.job.control.save())

    def test_unsave(self):
        with HTTMock(self.host.control_200):
            self.assertTrue(self.job.control.unsave())



