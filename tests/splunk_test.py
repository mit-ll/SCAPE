from __future__ import print_function
import unittest
import os
import getpass
import json

# import splunklib.client as client
# import splunklib.results as results
from nose.tools import set_trace, raises

import requests
from httmock import (
    all_requests, response, urlmatch, HTTMock
)

import scape.registry
import scape.splunk
import scape.splunklite as splunk

import mock_splunk

# class FakeJobs(object):
#     def create(self, query, **kwargs):
#         return []

# class FakeSplunk(object):
#     @property
#     def jobs(self):
#         return FakeJobs()
#     pass


class TestScapeSplunk(unittest.TestCase):
    def setUp(self):
        self.host = mock_splunk.SplunkHost(
            host='localhost',
            port=8089,
            username='admin',
            password='password1!',
        )
        self.service = self.host.service()


    # XXXX Need to fix this XXXX
    @unittest.skip("Need to fix splunklite handling search results")
    def test_splunk_registry(self):
        reg = scape.registry.Registry({
            'addc': scape.splunk.SplunkDataSource(
                splunk_service=self.service,
                metadata=scape.registry.TableMetadata({
                    'Source_Network_Address': {
                        'tags' : [ 'source'],
                        'dim': 'ip',
                    },
                    'Source_Port': {
                        'tags' : [ 'source'],
                        'dim': 'port',
                    },
                    'host': 'hostname:'
                }),
                index='addc',
                description="Test data source")
        })

        try:
            addc=reg['addc']
            with HTTMock(self.host.job_create_200, self.host.job_attr_200,
                         self.host.addc_results_200, self.host.control_200):
                for i,row in enumerate(addc.select('*').run().iter()):
                    self.assertTrue(row['host'].startswith('host'))
        except KeyboardInterrupt as err:
            set_trace()
    
    # def test_select():
    #     addc.select('*').run()
    #     addc.select(max_count=40).check()
