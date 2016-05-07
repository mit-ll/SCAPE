from __future__ import print_function
import os
import getpass
import json

# import splunklib.client as client
# import splunklib.results as results

import scape.registry as reg
import scape.splunk as scunk
import scape.splunklite as splunk

service = splunk.Service(
    host='localhost',
    port=8089,
    user='admin',
    password='password1!',
)

# class FakeJobs(object):
#     def create(self, query, **kwargs):
#         return []

# class FakeSplunk(object):
#     @property
#     def jobs(self):
#         return FakeJobs()
#     pass

reg = reg.Registry({
    'addc': scunk.SplunkDataSource(
        splunk_service=service,
        metadata=reg.TableMetadata({
            'Source_Network_Address': { 'tags' : [ 'source'], 'dim': 'ip' },
            'Source_Port': { 'tags' : [ 'source'], 'dim': 'port' },
            'host': 'hostname:'
            }),
        index='addc')
    })

addc=reg['addc']
    
def test_select():
    addc.select('*').run()
    addc.select(max_count=40).check()
