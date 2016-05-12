# -*- coding: utf-8 -*-
'''Minimal Splunk interface using REST API directly

Currently, the Splunk SDK does not support Python 3, and Splunk has
`no plans in the short term
<http://dev.splunk.com/view/python-sdk/SP-CAAAEUQ#python3>`_ to
provide Python 3 support.

In order to facilitate a consistent Python 2 and 3 development of
Scape, this module contains only the required Splunk query
functionality for Scape using the REST API directly. It follows the
SDK API somewhat, but it has a few substantial differences. The
primary difference is the lack of a ResultsReader object. Instead, the
returned Results object is an iterator that directly yields result
rows as dictionaries.

Example:

>>> service = Service( host='localhost', port=8089,
...                    username='myuser', password='pass1234' )
>>> job = service.jobs.create('search index=main source_computer=C149*')
>>> for row in job.results(count=0):
...     assert row['index'] == 'main'
...     assert row['source_computer'].startswith('C149')

Note:

  The purpose of this module is not to re-write the SDK, but just
  provide a temporary solution to the Python 3 support problem. The
  REST API is pretty straightforward, so minor additions to this
  module as needed shouldn't be too expensive. However, **the ultimate
  goal is to remove this module** as soon as Splunk updates their SDK
  to provide Python 3 support.

  The portions of this module that would be replaced directly by the
  Splunk SDK will be marked in the documentation as such:

  .. warning:: SDK Replacement

'''
import json
import warnings
import xml.etree.ElementTree as etree
import logging
import collections

import requests

_log = logging.getLogger('splunklite')
_log.addHandler(logging.NullHandler())
def _log_error_response(response):
    _log.error('code: {}'.format(response.status_code))
    _log.error('reason: {}'.format(response.reason))
    _log.error('content: {}'.format(response.text))

def post_unverified(*args, **kw):
    '''Simple wrapper around ``requests.post`` that allows for HTTPS GETs to
    hosts with self-signed SSL certificates sans warning messages

    Args:
      *args: args to pass to ``requests.post``
      **kw: kwargs to pass to ``requests.post``

    Examples:

    >>> response = post_unverified('http://localhost:8089/services/jobs')

    '''
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        kw['verify'] = False    # splunk uses self-signed certs
        return requests.post(*args, **kw)

def get_unverified(*args, **kw):
    '''Simple wrapper around ``requests.post`` that allows for HTTPS POSTs to
    hosts with self-signed SSL certificates sans warning messages

    Args:
      *args: args to pass to ``requests.get``
      **kw: kwargs to pass to ``requests.get``

    Examples:

    >>> response = get_unverified('http://localhost:8089/services/jobs')

    '''
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        kw['verify'] = False    # splunk uses self-signed certs
        return requests.get(*args, **kw)

class SplunkConnectError(Exception): pass
SplunkConnectionError = SplunkConnectError
class SplunkSessionKeyError(Exception): pass
class SplunkJobCreationError(Exception): pass
class SplunkAuthenticationError(Exception): pass

class HttpTrait(object):
    '''Simple HTTP trait that provides http_get and http_post behaviors
    for objects modeling Splunk REST endpoints

    '''
    @property
    def url(self):
        '''str: URL for this Splunk endpoint'''
        return ''
    @property
    def header(self):
        '''Dict[str, str]: Header information for HTTP connections. Normally
               just the authorization session key.

        '''
        return {}
    @property
    def params(self):
        '''Dict[str,str]: HTTP GET parameters to be encoded into the URL'''
        return {}
    @property
    def data(self):
        '''Dict[str,str]: HTTP POST data elements'''
        return {}

    def http_get(self, **kw):
        '''HTTP GET request behavior for Splunk REST endpoint object

        Args:
            **kw: Keyword arguments to be passed to ``requests.get``
                as GET parameters

        Uses get_unverified to suppress self-signed certificate
        warnings.

        '''
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
        '''HTTP POST request behavior for Splunk REST endpoint object

        Args:
            **kw: Keyword arguments to be passed to ``requests.post``
                as POST data

        Uses post_unverified to suppress self-signed certificate
        warnings.

        '''
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
    '''Splunk's REST service object.

    Handles session information and base URL generation.

    Args:
      host (str): hostname for search head 

      port (int): port for REST service

      username (str): Splunk login username

      password (str): Splunk login password

      protocol (str): communication protocol for REST service

    Example:

    >>> service = Service('localhost',8089, 'myuser', 'mypass')
    >>> job = service.jobs.create('search computer_name=C149*')

    .. warning:: SDK Replacement
    '''
    def __init__(self, host='localhost', port=8089,
                 username='', password='', protocol='https'):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.protocol = protocol
        

    @property
    def url(self):
        '''str: base URL for this service'''
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
            key = etree.fromstring(r.text).find('sessionKey').text
        else:
            raise SplunkSessionKeyError('could not get session key: {}'.format(r))
        return key
    
    _session_key = None
    @property
    def session_key(self):
        '''str: authentication session key for accessing REST service

        '''
        if self._session_key is None:
            self._session_key = self._get_session_key()
        return self._session_key

    @property
    def jobs(self):
        '''Jobs: reference to Jobs endpoint object'''
        return Jobs(self)
            

class Jobs(HttpTrait):
    '''Splunk REST endpoint object for /services/search/jobs

    Args:

      service (Service): this Jobs object's parent service object

    .. warning:: SDK Replacement
    '''
    def __init__(self, service):
        self._service = service

    def create(self, search_str, **kw):
        ''' Create search job

        Args:

          search_str (str): Splunk search string

        Returns:
          Job: new Job object for this search

        Example:

            >>> service = splunklite.Service()
            >>> job = service.jobs.create('search computer_name=C149*')

        .. warning:: SDK Replacement
        '''
        return Job(self, search_str, **kw)

    @property
    def url(self):
        '''str: URL for this jobs endpoint'''
        return '{base}/services/search/jobs'.format(base=self._service.url)

    @property
    def header(self):
        '''Dict[str,str]: HTTP header for this jobs endpoint'''
        return {
            'Authorization': 'Splunk {}'.format(self._service.session_key),
        }

debug = {}

_NS = {
    'atom': "http://www.w3.org/2005/Atom",
    'rest': "http://dev.splunk.com/ns/rest",
    'opensearch': "http://a9.com/-/spec/opensearch/1.1/",
}

def response_to_dict(response):
    '''Given a :class:`requests.Response` object, return an dictionary of
    the XML content containt in the response

    Args:

      response (requests.Response): HTTP response to convert to
        ElementTree

    Returns:

      Dict[ str, Union[str, List[str]] ]

    '''
    attrs = {}
    data = response.text
    if data:
        tree = etree.fromstring(response.text)
        def recurse(node, nobj=None):
            nobj = nobj if nobj is not None else {}
            if node is None:
                return nobj
            for key in node.findall('rest:key',_NS):
                name = key.get('name')
                cdict = key.find('rest:dict',_NS) 
                clist = key.find('rest:list',_NS)
                if cdict is not None:
                    nobj[name] = recurse(cdict, {})
                elif clist is not None:
                    nobj[name] = recurse(clist, [])
                else:
                    nobj[name] = key.text

            for item in node.findall('rest:item',_NS):
                cdict, clist = (item.find('rest:dict'),
                                item.find('rest:list'))
                if cdict is not None:
                    nobj.append(recurse(cdict, {}))
                elif clist is not None:
                    nobj.append(recurse(clist, []))
                else:
                    nobj.append(item.text)
            return nobj
        attrs = recurse(tree.find('./atom:content/rest:dict', _NS))

    return attrs

class Job(dict, HttpTrait):
    '''Splunk REST endpoint for a particular search job:
    /services/search/jobs/<jobid>

    Args:

      handler (Jobs): job handler, i.e. :class:`Jobs` object
        associated with this service's /search/jobs REST endpoint

      search_str (str): Splunk search string

      **kw: keyword arguments to be passed as POST data when creating
        this job

    Search job is created in Splunk during object construction.

    .. warning:: SDK Replacement
    '''
    def __init__(self, handler, search_str, **kw):
        self._handler = handler
        self._data = kw.copy()
        self._data['search'] = search_str

        self._id = None

        self._create_job()
        
    @property
    def url(self):
        '''str: URL for this search job: ``/services/search/jobs/<jobid>``'''
        return '{base}/{id}'.format(base=self._handler.url, id=self.id)

    @property
    def header(self):
        '''str: HTTP header for this search job'''
        return self._handler.header

    @property
    def data(self):
        '''str: HTTP POST data for creating this search job'''
        return self._data

    @property
    def id(self):
        '''str: Splunk search job id'''
        if self._id is None:
            raise SplunkConnectionError("connecting to a job that hasn't"
                                        " been created yet")
        return self._id

    def _create_job(self):
        '''Creates the search job, if it hasn't already been created

        Sends HTTP POST to /services/search/job with search string and
        whatever keyword arguments the user passed in at creation
        time. Sets the id property for this job.

        '''
        # return if already created
        if self._id is not None: return
        
        r = post_unverified(
            self._handler.url, headers=self.header,
            data=self.data,
        )
        if r.ok:
            e = etree.fromstring(r.text)
            sid = e.find('sid')
            if sid is not None:
                try:
                    self._id = sid.text
                except AttributeError:
                    _log_error_response(r)
                    raise
            else:
                _log_error_response(r)
                raise SplunkJobCreationError('could not retrieve SID')
        else:
            _log_error_response(r)
            if r.status_code == 400:
                # bad search
                raise SplunkJobCreationError('bad search command')
            elif r.status_code == 401:
                raise SplunkAuthenticationError('bad authentication')
            else:
                raise SplunkJobCreationError('unknown job creation error')

    def refresh(self):
        ''' Refresh the attributes of this :class:`Job`
        '''
        self.update(response_to_dict(self.http_get()))

    def is_ready(self):
        ''' Is this search fully created and running?

        Returns:
          bool: True if ready, False if not
        '''
        response = self.http_get()
        if response.status_code == 204:
            return False
        self.refresh()
        return self.get('dispatchState','') not in {
            'QUEUED', 'PARSING', '', 
        }


    def is_done(self):
        '''Is this job finished?

        Returns:
          bool: True if finished, False if not

        '''
        if not self.is_ready():
            return False
        return self['isDone'] == '1'

    @property
    def control(self):
        '''Control: object associated with this search :class:`Job`

        '''
        return Control(self)

    def results(self, **params):
        '''Get search :class:`Results` for this job

        Attempts to return preview if available while search job is
        finishing

        Args:

          **params: Keyword arguments to pass as parameters to HTTP
            GET that retrieves results (e.g. ``count=0`` for returning
            all results at once)

        Returns:
          Results: Iterator for results returned from search head

        Warning:

          SDK Replacement

          This method returns an object that is used differently than
          in the SDK. In the SDK, the results object isn't iterated
          over directly. Instead it is passed to a ResultsReader
          object and then iterated. 

        '''
        return Results(self, **params)

    def cancel(self):
        ''' Signals search head to cancel this job
        '''
        return self.control.cancel()

def _results_from_response(response):
    '''Given a response, return the results as a list of dictionaries

    Args:
      response (requests.Response): response from Splunk search head

    Returns:
      List[ Dict[str,str] ]: list if row results from Splunk
    '''
    data = json.loads(response.text)
    for message in data.get('messages',[]):
        _log.info('Message: {}'.format(message))
    return data.get('results',[])

class Results(collections.Iterator, HttpTrait):
    '''Iterator for results returned from
    ``/services/search/jobs/<jobid>/results``

    Args:
      job (Job): Search :class:`Job` to get results from

      **params: Keyword arguments to be passed to HTTP GET that
        retrieves results (e.g. ``count=0`` for returning all results
        at once)

    Attributes:
      index (int): index of row yielded

    Warning:
      SDK Replacement

    '''
    default_params = {
        'count': 100,
    }
    def __init__(self, job, **params):
        self._job = job
        
        self._params = self.default_params.copy()
        self._params.update(params)
        
        self.index = 0

    @property
    def url(self):
        ''' str: URL for this results object
        '''
        return '{base}/results_preview'.format(base=self._job.url)

    @property
    def header(self):
        ''' Dict[str, str]: HTTP header 
        '''
        return self._job.header

    @property
    def params(self):
        ''' Dict[str, str]: HTTP GET parameters for retrieving results
        '''
        params = self._params.copy()
        params.update({
            'output_mode': 'json',
            'offset': str(self.index),
        })
        return params

    def __iter__(self):
        return self

    _done = False
    def _row_generator(self):
        while not self._done:
            r = self.http_get()
            results = []
            if r.status_code == 200:
                results = _results_from_response(r)
                if results == []:
                    self._done = True
                else:
                    self._results = results
            else:
                _log_error_response(r)
                raise SplunkConnectError('could not retrieve results, see log')
            
            for row in results:
                yield row
                self.index += 1
            
    _generator = None
    def __next__(self):
        ''' Iterator for :class:`Results` object

        Yields:
          Dict[str, str]: result row of search

        '''
        if self._generator is None:
            self._generator = self._row_generator()
        return next(self._generator)
    next = __next__



class Control(HttpTrait):
    '''Splunk REST endpoint for job control: ``/services/search/jobs/<jobid>/control``

    Args:
      job (Job): Splunk search job


    Example:

    >>> job = service.jobs.create('search index="main" computer=C149*')
    >>> job.control.pause()     # to pause the running job
    >>> job.control.cancel()    # to cancel the paused job

    Warning:

      Does not exist in SDK. Instead the various control signals are
      sent directly from the Job object in the SDK.

    '''
    def __init__(self, job):
        self._job = job

    @property
    def url(self):
        ''' str: URL for this control endpoint '''
        return '{base}/control'.format(base=self._job.url)
    @property
    def header(self):
        ''' Dict[str, str]: HTTP header for control endpoint '''
        return self._job.header

    def _action(self, action, **kw):
        response = self.http_post(action=action, **kw)
        _log.debug('action: {} response: {} {}'.format(
            action, response.status_code, response.reason,
        ))
        return response.ok

    def pause(self):
        ''' Suspends the execution of the current search.

        Returns:
          bool: True if successful, False if not
        '''
        return self._action('pause')

    def unpause(self):
        ''' Resumes the execution of the current search, if paused.

        Returns:
          bool: True if successful, False if not
        '''
        return self._action('unpause')

    def finalize(self):
        '''Stops the search, and provides intermediate results to the
        /results endpoint.

        Returns:
          bool: True if successful, False if not
        '''
        return self._action('finalize')

    def cancel(self):
        ''' Stops the current search and deletes the result cache.

        Returns:
          bool: True if successful, False if not
        '''
        return self._action('cancel')

    def touch(self):
        ''' Extends the expiration time of the search to now + ttl

        Returns:
          bool: True if successful, False if not
        '''
        return self._action('touch')

    def setttl(self, ttl):
        ''' Change the ttl of the search. Arguments: ttl=<number>

        Args:
          ttl (Union[int, float]): time to live for this search

        Returns:
          bool: True if successful, False if not
        '''
        return self._action('setttl', ttl=ttl)

    def setpriority(self, priority):
        '''Sets the priority of the search process. Arguments:
        priority=<0-10>

        Args:
          priorty (int): priority for this search

        Returns:
          bool: True if successful, False if not
        '''
        return self._action('setpriority', priority=priority)

    def enablepreview(self):
        ''' Enable preview generation (may slow search considerably).

        Returns:
          bool: True if successful, False if not
        '''
        return self._action('enablepreview')

    def disablepreview(self):
        ''' Disable preview generation.

        Returns:
          bool: True if successful, False if not
        '''
        return self._action('disablepreview')

    def save(self):
        '''saves the search job, storing search artifacts on disk for 7
        days. Add or edit the default_save_ttl value in limits.conf to
        override the default value of 7 days.


        Returns:
          bool: True if successful, False if not
        '''
        return self._action('save')

    def unsave(self):
        '''Disables any action performed by save.

        Returns:
          bool: True if successful, False if not
        '''
        return self._action('unsave')


