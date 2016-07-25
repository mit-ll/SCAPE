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
#! /usr/bin/env python2.7
'''scape.utils.splunk

Utilities for dealing with splunk search heads.
'''

from __future__ import print_function
from __future__ import absolute_import
import os
import urllib
import httplib2
import time
from datetime import datetime,timedelta
from xml.dom import minidom
import random
import re
import traceback
import io
import sys
import csv
import StringIO
import logging

# Dependency:
#   splunklib: SDK from splunk
import splunklib.client
import splunklib.binding

import scape.config

import scape.utils

csv.field_size_limit(sys.maxint)

_log = logging.getLogger('scape.utils.splunk')
_log.addHandler(logging.NullHandler())

_FILE_TS_FMT = "%Y%m%d-%H%M%S"
def filename_ts(dt):
    return dt.strftime(_FILE_TS_FMT)

_ts_re = re.compile(r'\d{8}-\d{6}')
def path_to_dt(path):
    _,name = os.path.split(path)
    if _ts_re.search(path):
        ts = _ts_re.findall(path)[0]
        return datetime.strptime(ts,_FILE_TS_FMT)

def filename(index,dt,mode,count=None):
    if count is None:
        name = "{index}_{ets}.{mode}".format(
            index=index,ets=filename_ts(dt),mode=mode,
        )
    else:
        name = "{index}_{ets}_{count}.{mode}".format(
            index=index,ets=filename_ts(dt),mode=mode,count=count,
        )
    return name


class SplunkConnection(object):
    '''
    '''
    baseurl = scape.config.config['splunk']['baseurl']
    host = scape.config.config['splunk']['host']
    username = scape.config.config['splunk']['username']
    password = scape.config.config['splunk']['password']
    def __init__(self,retry=0,host=None,username=None,password=None):
        self.client = None
        self.sessionKey = None
        self.results = None
        self.retry = retry
        
        self.host = host or self.host
        self.username = username or self.username
        self.password = password or self.password

    def time(self,row):
        #return dateutil.parser.parse(row['_time'])
        fmt = '%Y-%m-%dT%H:%M:%S'
        return datetime.strptime(row['_time'][:-10],fmt)

    def index_last_time(self,index):
        days = 730
        query = ('search index="{index}" earliest=-{d}d@d |'
                 ' fields _time | head 1'.format(d=days,index=index))
        try:
            results = next(self.search(query))
        except StopIteration:
            _log.error('No data for last two years')
            return None
        sio = StringIO.StringIO(results)
        reader = csv.reader(sio)
        columns = next(reader)
        reader = csv.DictReader(sio,columns)
        row = next(reader)
        last_ts = self.time(row)
        return last_ts

    current_drift = 1
    def last_time(self,tried=0,drift=None):
        mins = 10+tried
        query = ('search index="{tindex}" earliest=-{m}m@m |'
                 ' fields _time | head 1'.format(
                     m=mins,tindex=scape.config.config['splunk']['time_index']
                 ))
        try:
            results = next(self.search(query))
        except StopIteration:
            time.sleep(10)
            if tried>10:
                raise KeyError('Too much drift, please fix')
                #return datetime.now()
            return self.last_time(tried+1)
        sio = StringIO.StringIO(results)
        reader = csv.reader(sio)
        columns = next(reader)
        reader = csv.DictReader(sio,columns)
        row = next(reader)
        last_ts = self.time(row)
        return last_ts

    def http(self):
        return httplib2.Http(disable_ssl_certificate_validation=True)
    def auth(self):
        http = self.http()
        _,content = http.request(
            self.baseurl+'/services/auth/login','POST',
            headers={},
            body=urllib.urlencode({'username':self.username,
                                   'password':self.password})
            )
        self.sessionKey = minidom.parseString(content)\
            .getElementsByTagName('sessionKey')[0].childNodes[0].nodeValue
        _log.info('session key: %s',self.sessionKey)
    def get_service(self,keep_trying=False):
        tries = 30
        while tries or keep_trying:
            try:
                _log.info(scape.utils.lines(
                        'Connecting to splunk head: {}'.format(self.host)
                        ))
                service = splunklib.client.connect(
                    host=self.host,username=self.username,
                    password=self.password,
                    )
                return service
            except:
                _log.error('ERROR CONNECTING TO SPLUNK ({})'.format(tries))
                _log.error(traceback.format_exc())
                o,_,_ = scape.utils.sh('arp -a')
                netstat = [l.strip() for l in o.splitlines()]
                _log.error(scape.utils.lines(*netstat))
                time.sleep(random.random()*30)
                tries -= 1
        _log.error('TRYING ONCE MORE')
        service = splunklib.client.connect(
            host=self.host,username=self.username,
            password=self.password,
            )
        return service
    def search(self,query,output_mode='csv',**kw):
        #service = self.get_service()
        service = self.get_service(True) # force retries in face of
                                         # socket.error until we
                                         # figure out what's going
                                         # on...
        _log.info('\nquery: %s',query)
        sleep = 10

        service.parse(query,parse_only=True)

        job = service.jobs.create(query,max_count=2**32)
        sleep = 0.1
        sleep_reset = False
        try:
            while True:
                while True:
                    time.sleep(5)
                    refresh_tries = 0
                    try:
                        job.refresh()
                        break
                    except AttributeError:
                        if refresh_tries > 20:
                            raise
                        _log.error('problem querying job... %s retries',refresh_tries)
                        refresh_tries += 1
                        time.sleep(4)
                while True:
                    try:
                        stats = {k:job[k] for k in ['isDone','doneProgress','scanCount',
                                                    'eventCount','resultCount']}
                        break
                    except AttributeError:
                        time.sleep(2)
                        job.refresh()
                    
                progress = float(stats['doneProgress'])*100
                if progress > 95 and not sleep_reset:
                    sleep = 1
                    sleep_reset = True
                scanned,matched,results = map(
                    int,map(stats.get,['scanCount','eventCount','resultCount'])
                    )
                status = ('{progress:03.1f}%'
                          ' | {scanned} scanned'
                          ' | {matched} matched'
                          ' | {results} results').format(**locals())
                _log.info(scape.utils.lines('',status,''))
                if stats['isDone'] == '1':
                    break
                _log.info('sleeping %s secs',sleep)
                time.sleep(sleep)
                sleep *= 2
                if sleep>30:
                    sleep = 30
        except:
            job.delete()
            raise
        
        class ResultGenerator(object):
            deleted = False
            def _delete_job(self):
                if not self.deleted:
                    job.delete()
                    self.deleted = True
            def __del__(self):
                _log.info('deleting %s',job)
                self._delete_job()
            def generator(self):
                count = 0
                offset = -1
                try:
                    results = job.results(count=count,output_mode=output_mode)
                    content = results.read()
                    while content:
                        yield content
                        offset += 50000
                        results = job.results(count=count,
                                              output_mode=output_mode,
                                              offset=offset)
                        content = results.read()
                finally:
                    self._delete_job()
                                      
        return ResultGenerator().generator()
    
    def indexes(self):
        query = ('| eventcount summarize=false index=*'
                 ' | dedup index | fields index')
        results = self.search(query)
        retlist = []
        for r in results:
            reader = csv.reader(io.StringIO(unicode(r)))
            retlist.extend([str(v[0]) for v in list(reader)[1:]])
        return retlist

    def query(self,index=None,start=None,end=None,output_mode='csv',
              fields=None,explicit_query=None,
              file_size=0,head=None,**kw):
        if explicit_query:
            query = explicit_query
        else:
            fmt = '%m/%d/%Y:%H:%M:%S'
            query = 'search index="{index}" earliest={start} {latest} | fields {fields} {head}'
            if not fields:
                fields = '*'
            else:
                fields = ','.join(fields)
            if end is None:
                latest = ''
            else:
                latest = 'latest={}'.format(end.strftime(fmt))
            if type(start) is datetime:
                start=start.strftime(fmt)
            elif type(start) is timedelta:
                start = '-{}s@s'.format(int(start.total_seconds()))
            if head is not None:
                head = '| head {head}'.format(head=head)
            else:
                head = ''
            query = query.format(index=index,start=start,
                                 latest=latest,
                                 fields=fields,head=head)

        results = self.search(query,output_mode)

        if not results:
            _log.info('No results')
            return

        _log.info('FILESIZE: %s',file_size)
        if not file_size:
            # put all content into a single string
            content = next(results)
            for r in results:
                sio = StringIO.StringIO(r)
                reader = csv.reader(sio)
                columns = next(reader)
                content += r[sio.tell():]
            sio = StringIO.StringIO(content)
            reader = csv.reader(sio)
            columns = next(reader)
            reader = csv.DictReader(sio,columns)
            row = next(reader)
            name = filename(index,self.time(row),output_mode)
            yield {'content':content,'name':name}
        else:
            extents = [None,None]
            names = {}
            totalSize = 0
            allColumns = set()
            allRows = []

            def writeresults(rows,columns,extents):
                name = filename(index,extents[0],output_mode)
                if name in names:
                    names[name] += 1
                    name = filename(index,extents[0],output_mode,names[name])
                else:
                    names[name] = 0
                writer_io = StringIO.StringIO()
                writer = csv.DictWriter(writer_io,sorted(columns))
                writer.writeheader()
                writer.writerows(rows)
                return {'content':writer_io.getvalue(),
                        'name':name}

            for content in results:
                reader_io = StringIO.StringIO(content)
                reader = csv.reader(reader_io)
                columns = next(reader)
                allColumns.update(columns)
                totalSize += sum(len(c) for c in columns)+len(columns)-1
                reader = csv.DictReader(reader_io,columns)
                for row in reader:
                    if extents[0] is None:
                        try:
                            extents[0] = self.time(row)
                        except KeyError:
                            break
                    allRows.append(row)
                    totalSize += sum(len(v) for v in row.values())+len(row)-1
                    if totalSize > file_size:
                        # write out file
                        extents[1] = self.time(row)
                        yield writeresults(allRows,allColumns,extents)
                        extents = [None,None]
                        totalSize = sum(len(c) for c in columns)+len(columns)-1
                        allRows = []
                        allColumns = set(columns)
            # write out file
            if totalSize:
                extents[1] = self.time(row)
                yield writeresults(allRows,allColumns,extents)

    def query_rows(self,*a,**kw):
        for result in self.query(*a,**kw):
            rfp = StringIO.StringIO(result['content'])
            for row in scape.utils.csv_rows(rfp):
                yield row

def indexes():
    scon = SplunkConnection()
    return scon.indexes()
