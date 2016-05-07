from __future__ import print_function
from time import sleep
import splunklib.client as client
import splunklib.results as results
import json
import scape.registry as reg

def load_splunk_registry(service, json_filename):
      with open(json_filename, 'rt') as fp:
          js = json.load(fp)
          def ds(index):
              return SplunkDataSource(service, reg.TableMetadata(js[index]), index)
          d = {index:ds(index) for index, fields in js.items()}
          return reg.Registry(d)

def _extra_fields(table_meta, field_counts):
    fields = set(field_counts.keys())
    return [f for f in table_meta.field_names if f not in fields]

def _missing_fields(table_meta, field_counts, ignore=[]):
    fields = set(table_meta.field_names)
    return {f:{'tags':[], 'dim':None} for f in field_counts.keys()
            if f not in fields and f not in ignore}

class SplunkDataSource(reg.DataSource):
    def __init__(self, splunk_service, metadata, index):
        super(SplunkDataSource, self).__init__(metadata, {
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
        for o in dir(select):
            if o in attrs:
                kwargs[o] = getattr(select, o)
        return kwargs

    def debug(self, select):
        print("splunk_params=", self._get_splunk_params(select))
        print("condition=", select._condition)
        print("select_fields=", select._fields)
        cond = self._rewrite(select._condition)
        search_query = _go(cond)
        if select._fields:
            fs = set()
            for selector in select._fields:
                xs = [f.name for f in self._metadata.fields_matching(selector)]
                print(xs)
                fs = fs.union(set(xs))
            fields = "| fields " + ", ".join(fs)
        else:
            fields = ""

        query = "search index={} {} {}".format(self._index, search_query, fields)
        print("splunk query=[", query, "]")

    def check_select(self, select):
        self.debug(select)

    def run(self, select):
        self.check_query(select._condition)
        cond = self._rewrite(select._condition)
        search_query = _go(cond)
        query = "search index={} {}".format(self._index, search_query)
        kwargs = self._get_splunk_params(select)
#        print(query)
#        print(kwargs)
        job = self._service.jobs.create(query, **kwargs)
        return SplunkResults(job)

#        return synchronous_get(self._service, "search index={} {}".format(self._index, search_query), **kwargs)

def get_all_index_fields(service):
    ixs = service.indexes.list()
    res = {}
    for ix in ixs:
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
    xs = synchronous_get(service, query, **kw)
    return {f['field']:f['count'] for f in xs}

class SplunkResults():
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
    #             print(r)
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
#             print(r)
    job.cancel()
    return res

def _paren(xs, sep):
    if len(xs)==1:
        return xs[0]
    else:
        s = ' ' + sep + ' '
        return '(' + s.join(xs) + ')'

def _go(cond):
    if isinstance(cond, reg.Equals):
        return '({}="{}")'.format(cond.lhs.name, cond.rhs)
#    elif isinstance(cond, MatchesCond):
#        return "({}={})".format(cond.lhs, cond.rhs)
    elif isinstance(cond, reg.Or):
        return _paren([_go(x) for x in cond.xs], 'OR')
    elif isinstance(cond, reg.And):
        return _paren([_go(x) for x in cond.xs], 'AND')


def _save_to_json(x, filename):
    with open(filename, 'wt') as fp:
         json.dump(x, fp, sort_keys=True, indent=4)

def _read_json(filename):
    with open(filename, 'rt') as fp:
        return json.load(fp)