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

from __future__ import print_function
from time import sleep
import collections
import json

import splunklib.results as results

import scape.registry as reg

def load_splunk_registry(service, json_filename):
    with open(json_filename, 'rt') as fp:
        js = json.load(fp)
        def datasource(index,ds):
            description = ds['description'] if 'description' in ds else ""
            metadata = reg.TableMetadata(ds['fields'])
            return SplunkDataSource(service, metadata, index, description)
        d = {index:datasource(index,ds) for index, ds in js.items()}
        return reg.Registry(d)

def _extra_fields(table_meta, field_counts):
    fields = set(field_counts.keys())
    return [f for f in table_meta.field_names if f not in fields]

def _missing_fields(table_meta, field_counts, ignore=[]):
    fields = set(table_meta.field_names)
    return {f:{'tags':[], 'dim':None} for f in field_counts.keys()
            if f not in fields and f not in ignore}

_omit_fields=['_indextime', '_kv', '_raw','_serial','_sourcetype', '_time']

class SplunkDataSource(reg.DataSource):
    def __init__(self, splunk_service, metadata, index, description=""):
        super(SplunkDataSource, self).__init__(metadata, description, {
            '==': reg.Equals,
            '=~':  reg.MatchesCond
        })
        self._service = splunk_service
        self._index = index
        self._name = index

    def _get_splunk_params(self, select):
        attrs = ['earliest', 'earliest_time',
                 'index_earliest', 'index_latest',
                 'latest', 'latest_time',
                 'max_count', 'max_time',
                 'status_buckets',
                 'timeout']
        kwargs = {}
        for k,v in select._ds_kwargs.items():
            if k in attrs:
                kwargs[k] = v
        return kwargs

    def _fields_pipe(self, select):
        fields_pipe = ""
        if select.fields:
            field_names = self._field_names(select)
            fields_pipe = "| fields " + ", ".join(field_names)
        return fields_pipe

    def _pipe_omitted_fields(self,select):
        fs = list(set(_omit_fields) - set(select.fields))
        return "| fields - " + ", ".join(fs)

    def debug_select(self, select):
        self.check_select(select, debug=True)

    def check_select(self, select, debug=False):
        cond = self._rewrite(select.condition)
        search_query = _go(cond)
        fields = self._fields_pipe(select)
        omitted_fields = self._pipe_omitted_fields(select)
        query = "search index={} {} {} {}".format(self._index, search_query, fields, omitted_fields)
        kwargs = self._get_splunk_params(select)
        if debug:
            print("kwargs=", kwargs)
            print("splunk_params=", self._get_splunk_params(select))
            print("condition=", select.condition)
            print("search_query=", search_query)
            print("fields=", select.fields)
            print("omitted_fields=", omitted_fields)
            print("splunk query=[", query, "]")

    def run(self, select):
        cond = self._rewrite(select.condition)
        search_query = _go(cond)
        fields = self._fields_pipe(select)
        omitted_fields = self._pipe_omitted_fields(select)
        query = "search index={} {} {} {}".format(self._index, search_query, fields, omitted_fields)

        kwargs = self._get_splunk_params(select)
        job = self._service.jobs.create(query, **kwargs)
        return SplunkResults(job)

#        return synchronous_get(self._service, "search index={} {}".format(self._index, search_query), **kwargs)

def get_all_index_fields(service):
    indexes = service.indexes.list()
    res = {}
    for index in indexes:
        if int(ix.state['content']['totalEventCount']) > 0:
            print("Getting ", ix.name)
            fields = get_splunk_fields(service, ix.name)
            res[ix.name] = fields
            print("Finished ", ix.name)
        else:
            print("Skipping ", ix.name, " no events")
    return res

def get_splunk_fields(service, index, max=30000, inclusion_percent=0.01):
    ""
    query = "search index={} earliest=-1d | head {} | fieldsummary | table field count | where count > {}".format(index, max, max * inclusion_percent)
    kw = { 'exec_mode' : 'normal', 'count' : 0 }
    fields = synchronous_get(service, query, **kw)
    return {f['field']:f['count'] for f in fields}

class SplunkResults(collections.Iterator):
    def __init__(self, job):
        self._job = job

    def is_done(self):
        job = self._job
        while not job.is_ready():
            pass
        return self._job['isDone']=='1'

    def print_progress(self):
        job = self._job
        done = self.is_done()
        stats = {'isDone': job['isDone'],
                 'doneProgress': job['doneProgress'],
                 'scanCount': job['scanCount'],
                 'eventCount': job['eventCount'],
                 'resultCount': job['resultCount']}
        progress = float(stats['doneProgress'])*100
        scanned = int(stats['scanCount'])
        matched = int(stats['eventCount'])
        results = int(stats['resultCount'])
        status = ("\r%03.1f%% | %d scanned | %d matched | %d results" % (progress, scanned, matched, results))
        print(status)
        return done

    def get_progress(self, verbose):
        done = self.is_done()
        if verbose:
            return self.print_progress()
        return done

    _iter = None
    def __iter__(self):
        if self._iter is None:
            self._iter = self.iter(verbose=False)
        return self._iter

    def __next__(self):
        return next(self._iter)
    next = __next__
    
    def iter(self, verbose=True):
        """An iterator of results"""
        while not self.is_done():
            if verbose:
                self.print_progress()
            sleep(2)

        rr = results.ResultsReader(self._job.results(count=0))
        for r in rr:
            if isinstance(r, results.Message):
                print(" {} {}".format(r.type, r.message))
            elif isinstance(r, dict):
                 yield r

        self.cancel()

    def cancel(self):
        self._job.cancel()


def _splunk_jobs(service, query, **kwargs):
    job = service.jobs.create(query, **kwargs)

def synchronous_get(service, query, **kwargs):
    job = service.jobs.create(query, **kwargs)
    print("query=", query, "kwargs=", kwargs)
    while not job.is_done():
        print('.', end='')
        sleep(2)

    rr = results.ResultsReader(job.results(count=0))
    res = []
    for r in rr:
        if isinstance(r, results.Message):
            print(" {} {}".format(r.type, r.message))
        elif isinstance(r, dict):
            res.append(r)
    
    job.cancel()
    return res

def _paren(parts, sep):
    if len(parts)==1:
        return parts[0]
    else:
        s = ' ' + sep + ' '
        return '(' + s.join(parts) + ')'

def _go(cond):
    if isinstance(cond, reg.Equals):
        return '({}="{}")'.format(cond.lhs.name, cond.rhs)
#    elif isinstance(cond, MatchesCond):
#        return "({}={})".format(cond.lhs, cond.rhs)
    elif isinstance(cond, reg.Or):
        return _paren([_go(x) for x in cond.parts], 'OR')
    elif isinstance(cond, reg.And):
        return _paren([_go(x) for x in cond.parts], 'AND')
    elif isinstance(cond, reg.TrueCondition):
        return ""
    else:
        raise ValueError((cond,type(cond)))


def _save_to_json(x, filename):
    with open(filename, 'wt') as fp:
         json.dump(x, fp, sort_keys=True, indent=4)

def _read_json(filename):
    with open(filename, 'rt') as fp:
        return json.load(fp)
