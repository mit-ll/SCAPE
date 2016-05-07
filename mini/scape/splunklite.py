import json
import warnings
import xml.etree.ElementTree as etree
import logging
import collections

_log = logging.getLogger('splunklite')
_log.addHandler(logging.NullHandler())

import requests

def xml_response(r):
    return etree.fromstring(r.text)

def post_unverified(*args, **kw):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        kw['verify'] = False    # splunk uses self-signed certs
        return requests.post(*args, **kw)

def get_unverified(*args, **kw):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        kw['verify'] = False    # splunk uses self-signed certs
        return requests.get(*args, **kw)

class SplunkConnectError(Exception): pass

class HttpTrait(object):
    @property
    def url(self):
        return ''
    @property
    def header(self):
        return {}
    @property
    def params(self):
        return {}
    @property
    def data(self):
        return {}

    def http_get(self, **kw):
        url = self.url
        _log.debug('url: {}'.format(url))

        header = self.header
        _log.debug('header: {}'.format(header))

        params = self.params.copy()
        params.update(kw)
        _log.debug('params: {}'.format(params))

        return get_unverified(
            url, headers=header, params=params,
        )

    def http_post(self, **kw):
        url = self.url
        _log.debug('url: {}'.format(url))

        header = self.header
        _log.debug('header: {}'.format(header))

        data = self.data.copy()
        data.update(kw)
        _log.debug('data: {}'.format(data))

        return post_unverified(
            url, headers=header, data=data,
        )
    
    
class Service(object):
    def __init__(self, host='localhost', port=8089,
                 username='', password='', protocol='https'):
        self.protocol = protocol
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        

    @property
    def url(self):
        return '{prot}://{host}:{port}'.format(
            prot=self.protocol, host=self.host, port=self.port,
        )

    def _get_session_key(self):
        r = post_unverified(
            '{base}/services/auth/login'.format(base=self.url),
            data={'username': self.username,
                  'password': self.password},
        )
        if r.ok:
            key = xml_response(r).find('sessionKey').text
        else:
            raise SplunkConnectError('could not get session key: {}'.format(r))
        return key
    
    _session_key = None
    @property
    def session_key(self):
        if self._session_key is None:
            self._session_key = self._get_session_key()
        return self._session_key

    @property
    def jobs(self):
        return Jobs(self)
            

class Jobs(HttpTrait):
    def __init__(self, service):
        self._service = service

    def create(self, job_str, **kw):
        return Job(self, job_str, **kw)

    @property
    def url(self):
        return '{base}/services/search/jobs'.format(base=self._service.url)

    @property
    def header(self):
        return {
            'Authorization': 'Splunk {}'.format(self._service.session_key),
        }

debug = {}
class Job(dict, HttpTrait):
    ns = {
        'atom': "http://www.w3.org/2005/Atom",
        'rest': "http://dev.splunk.com/ns/rest",
        'opensearch': "http://a9.com/-/spec/opensearch/1.1/",
    }
    def __init__(self, handler, job_str, **kw):
        self._handler = handler
        self.job_str = job_str

        header = self._handler.header

        data = kw.copy()
        data.update({
            'search':job_str,
        })
        _log.debug(data)

        r = post_unverified(
            self._handler.url, headers=header,
            data=data,
        )
        if r.ok:
            e = xml_response(r)
            sid = e.find('sid')
            if sid is not None:
                try:
                    self.id = sid.text
                except AttributeError:
                    _log.error(r.text)
                    raise
            else:
                _log.error(sid)
                _log.error(r.text)
                raise SplunkConnectError('could not retrieve SID')
        else:
            pass
            # raise SplunkConnectError('could not connect to splunk job')

    def response_to_dict(self, response):
        tree = xml_response(response)
        def recurse(node, nobj=None):
            nobj = nobj if nobj is not None else {}
            if node is None:
                return nobj
            for key in node.findall('rest:key',self.ns):
                name = key.get('name')
                cdict = key.find('rest:dict',self.ns) 
                clist = key.find('rest:list',self.ns)
                if cdict is not None:
                    nobj[name] = recurse(cdict, {})
                elif clist is not None:
                    nobj[name] = recurse(clist, [])
                else:
                    nobj[name] = key.text

            for item in node.findall('rest:item',self.ns):
                cdict, clist = (item.find('rest:dict'),
                                item.find('rest:list'))
                if cdict is not None:
                    nobj.append(recurse(cdict, {}))
                elif clist is not None:
                    nobj.append(recurse(clist, []))
                else:
                    nobj.append(item.text)
            return nobj
        attrs = recurse(tree.find('./atom:content/rest:dict', self.ns))
        return attrs

    def refresh(self):
        self.update(self.response_to_dict(self.http_get()))

    def is_ready(self):
        response = self.http_get()
        if response.status_code == 204:
            return False
        self.refresh()
        return self.get('dispatchState','') not in {
            'QUEUED', 'PARSING', '', 
        }


    def is_done(self):
        if not self.is_ready():
            return False
        return self['isDone'] == '1'

    @property
    def url(self):
        return '{base}/{id}'.format(base=self._handler.url, id=self.id)

    @property
    def header(self):
        return self._handler.header

    @property
    def control(self):
        return Control(self)

    def results(self, **params):
        return Results(self, **params)

    def cancel(self):
        return self.control.cancel()

def results_from_response(response):
    data = json.loads(response.text)
    for message in data.get('messages',[]):
        _log.info('Message: {}'.format(message))
    return data.get('results',[])

class Results(collections.Iterator, HttpTrait):
    default_params = {
        'count': 100,
    }
    def __init__(self, job, **params):
        self._job = job
        
        self._params = self.default_params.copy()
        self._params.update(params)
        
        self.index = 0

    def __iter__(self):
        return self

    _done = False
    def _row_generator(self):
        while not self._done:
            r = self.http_get()
            if r.status_code == 200:
                results = results_from_response(r)
                if results == []:
                    self._done = True
                else:
                    self._results = results
            else:
                _log.error('Bad results: {} {} {}'.format(r,r.reason,r.text))
                raise SplunkConnectError('could not retrieve results')
            
            for row in results:
                self.index += 1
                yield row
            
    _generator = None
    def __next__(self):
        if self._generator is None:
            self._generator = self._row_generator()
        return next(self._generator)
    next = __next__

    @property
    def url(self):
        return '{base}/results_preview'.format(base=self._job.url)
    @property
    def header(self):
        return self._job.header

    @property
    def params(self):
        data = self._params.copy()
        data.update({
            'output_mode': 'json',
            'offset': str(self.index),
        })
        return data



class Control(HttpTrait):
    def __init__(self, job):
        self._job = job

    @property
    def url(self):
        return '{base}/control'.format(base=self._job.url)
    @property
    def header(self):
        return self._job.header

    def action(self, action, **kw):
        response = self.http_post(action=action, **kw)
        return response.ok

    def pause(self):
        ''' Suspends the execution of the current search.'''
        self.action('pause')

    def unpause(self):
        ''' Resumes the execution of the current search, if paused.'''
        self.action('unpause')

    def finalize(self):
        '''Stops the search, and provides intermediate results to the
        /results endpoint.'''
        self.action('finalize')

    def cancel(self):
        ''' Stops the current search and deletes the result cache.'''
        self.action('cancel')

    def touch(self):
        ''' Extends the expiration time of the search to now + ttl'''
        self.action('touch')

    def setttl(self, ttl):
        ''' Change the ttl of the search. Arguments: ttl=<number>'''
        self.action('setttl', ttl=ttl)

    def setpriority(self, priority):
        '''Sets the priority of the search process. Arguments:
        priority=<0-10>'''
        self.action('setpriority', priority=priority)

    def enablepreview(self):
        ''' Enable preview generation (may slow search considerably).'''
        self.action('enablepreview')

    def disablepreview(self):
        ''' Disable preview generation.'''
        self.action('disablepreview')

    def save(self):
        '''saves the search job, storing search artifacts on disk for 7
        days. Add or edit the default_save_ttl value in limits.conf to
        override the default value of 7 days.

        '''
        self.action('save')

    def unsave(self):
        '''Disables any action performed by save.'''
        self.action('unsave')


# class ResultsReader(collections.Iterator):
#     def __init__(self, results):
#         pass

#     def __iter__(self):
#         return self

#     def __next__(self):
#         pass
#     next = __next__
