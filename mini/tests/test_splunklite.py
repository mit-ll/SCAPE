import unittest

from httmock import (
    all_requests, response, urlmatch, HTTMock
)

import scape.splunklite as slite

class TestSplunkLiteService(unittest.TestCase):
    def setUp(self):
        pass

    def test_service_url(self):
        s = slite.Service('host1', 9999, protocol='http')
        self.assertEqual(s.url, 'http://host1:9999')

    @all_requests
    def session_key_response_content(self, url, request):
        headers = {
            'cache-control': 'no-store, no-cache, must-revalidate, max-age=0',
            'connection': 'Keep-Alive',
            'content-length': '154',
            'content-type': 'text/xml; charset=UTF-8',
            'date': 'Wed, 11 May 2016 02:14:12 GMT',
            'expires': 'Thu, 26 Oct 1978 00:00:00 GMT',
            'server': 'Splunkd',
            'x-content-type-options': 'nosniff',
            'x-frame-options': 'SAMEORIGIN',
        }


    def test_session_key(self):
        s = slite.Service('host1', 9999, protocol='http')
        self.assertEqual(s.url, 'http://host1:9999')


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
