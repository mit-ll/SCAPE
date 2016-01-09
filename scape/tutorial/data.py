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
import random
import csv
import json
from datetime import datetime, timedelta
import calendar
import hashlib

import scape.utils.args

datetime_format = '%Y-%m-%d %H:%M:%S'

#value models

domains = [
    {'pop': 10, 'domain':'google.com', 'ip': ['10.10.10.1', '10.10.10.2', '10.10.10.3', '10.10.10.4', '10.10.10.5']},
    {'pop': 9, 'domain':'facebook.com', 'ip': ['11.10.10.1', '11.10.10.2', '11.10.10.3', '11.10.10.4', '11.10.10.5']},
    {'pop': 9, 'domain':'yahoo.com', 'ip': ['13.10.10.1', '13.10.10.2', '13.10.10.3', '13.10.10.4', '13.10.10.5']},
    {'pop': 7, 'domain':'mit.edu', 'ip': ['12.10.10.1', '12.10.10.2', '12.10.10.3', '12.10.10.4', '12.10.10.5']},
    {'pop': 5, 'domain':'cnn.com', 'ip': ['14.10.10.1', '14.10.10.2', '14.10.10.3']},
    {'pop': 4, 'domain':'imgur.com', 'ip': ['15.10.10.1', '15.10.10.2']},
    {'pop': 2, 'domain':'villanova.edu', 'ip': ['16.10.10.1']},
    {'pop': 1, 'domain':'upenn.edu', 'ip': ['17.10.10.1']}
]
#pop = sqrt of relative popularity

users = [
    {'pop':10, 'user':'user0', 'ip':'192.168.0.1', 'mac':'3c:07:54:5f:5a:72'},
    {'pop':10, 'user':'user1', 'ip':'192.168.0.2', 'mac':'3c:07:55:5f:5a:12'},
    {'pop': 9, 'user':'user2', 'ip':'192.168.0.3', 'mac':'3c:07:56:5f:5a:45'},
    {'pop': 8, 'user':'user3', 'ip':'192.168.0.4', 'mac':'3c:07:57:5f:5a:73'},
    {'pop': 7, 'user':'user4', 'ip':'192.168.0.5', 'mac':'3c:07:58:5f:5a:77'},
    {'pop': 6, 'user':'user5', 'ip':'192.168.0.6', 'mac':'3c:07:59:5f:5a:34'},
    {'pop': 5, 'user':'user6', 'ip':'192.168.0.7', 'mac':'3c:07:5a:5f:5a:63'},
    {'pop': 4, 'user':'user7', 'ip':'192.168.0.8', 'mac':'3c:07:5b:5f:5a:23'},
    {'pop': 3, 'user':'user8', 'ip':'192.168.0.9', 'mac':'3c:07:5c:5f:5a:54'},
    {'pop': 2, 'user':'user9', 'ip':'192.168.0.10', 'mac':'3c:07:5d:5f:5a:22'},
    {'pop':1, 'user':'user10', 'ip':'192.168.0.11', 'mac':'3c:07:5e:5f:5a:10'},
    {'pop':1, 'user':'user11', 'ip':'192.168.0.12', 'mac':'3c:07:5f:5f:5a:01',
     'uniform': True}
]
#pop = sqrt of hits per hour

prefixes = ['www', 'mail', 'www2', 'm']
    
def seconds_to_timestamp(day, offset_seconds):
    """Day is a string in the form 'YYYY-MM-DD', offset_seconds is seconds after midnight"""
    delta = timedelta(0, offset_seconds)
    dt = scape.utils.args.date_convert(day)
    return (dt + delta).strftime(datetime_format)

def pick_index(weights):
    total = sum(weights)
    r = random.random() * total
    c = 0
    i = 0
    for w in weights:
        c += w
        if r < c:
            return i
        i += 1
    return i

def generate_rows(day,seed=None):
    dhcp, addc, web = [],[],[]
    if seed is not None:
        random.seed(seed)
    for u in users:
        if ('uniform' in u) and (u['uniform']):
            start_time = 0
            duration = 24*60*60 #all day
            # print u['user'],start_time
        else:
            start_time = random.gauss(8, 1.5) * 60 * 60
            end_time = random.gauss(17, 1.5) * 60 * 60
            # print u['user'],start_time, end_time
            duration = int(end_time - start_time)
            if duration < 0:
                duration = -duration
                print 'Fixing negative duration'
        #create dhcp event
        event = {}
        event['src_mac'] = u['mac']
        event['src_ip'] = u['ip']
        event['dhcp_command'] = 'request'
        event['datetime'] = seconds_to_timestamp(day, start_time - 30)
        dhcp.append(event)
        
        #create login event
        event = {}
        event['account_name'] = u['user']
        event['datetime'] = seconds_to_timestamp(day, start_time)
        event['client_address'] = u['ip']
        event['source_network_address'] = u['ip']
        event['taskcategory'] = 'logon'
        addc.append(event)
        
        #create a bunch of web requests
        hits_per_hour = u['pop']**2
        hits = hits_per_hour*duration/3600
        for i in range(hits):
            time = int(random.random()*duration)
            # if i<10: print time
            event = {}
            event['datetime'] = seconds_to_timestamp(day, time+start_time)
            event['src_ip'] = u['ip']
            event['user'] = u['user']
            dest = domains[pick_index(map(lambda x: x['pop']**2, domains))]
            event['srv_ip'] = dest['ip'][int(random.random() * len(dest['ip']))]
            event['req_domain'] = dest['domain']
            event['req_fqdn'] = (
                prefixes[int(random.random() * len(prefixes))] + 
                '.' + dest['domain']
            )
            event['bytes_to_client'] = str(random.randint(1000,300000))
            event['bytes_from_client'] = str(random.randint(500,2000))
            web.append(event)

    return dhcp, addc, web
            
if __name__ == "__main__":
    generate('2014-08-01')
    
    
