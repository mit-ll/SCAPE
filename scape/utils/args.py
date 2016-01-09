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
'''scape.utils.args

Helper functions for dealing with command-line arguments within the
scape system.
'''
import os
import re
import argparse
from datetime import datetime,timedelta,date

import dateutil.parser

def existing_path(error_msg=None):
    '''Path checking type for arguments

    If path is provided, convert to absolute path and check for
    existence. If it doesn't exist, raise argparse.ArgumentTypeError.

    >>> import argparse
    >>> parser = argparse.ArgumentParser()
    >>> parser.add_argument(
    ...     '-p','--path',
    ...     type = existing_path('Path ({path}) does not exist'),
    ... )
    >>> ns,_ = parser.parse_args(['-p','good_file_name'])
    >>> ns.path
    '/abs/path/to/good_file_name'

    '''
    def check(path):
        if path is not None:
            path = os.path.abspath(path)
            if not os.path.exists(path):
                if error_msg is not None:
                    msg = error_msg.format(path=path)
                else:
                    msg = 'Given path must exist: {}'.format(path)
                raise argparse.ArgumentTypeError(msg)
            return path
    return check

def date_convert_help():
    msg = (
        "A date can be given in most know date/time formats,"
        " or it can be given as a relative"
        " time period in #w#d#h#m#s form (i.e. 2w1d5h for 2 weeks"
        " + 1 day + 5 hours)."
        )
    return msg

def delta_convert(dobject):
    '''String to datetime/timedelta transform type for arguments

    timedelta objects passed in are returned back. If None is passed
    in, it is returned back, as well. If it cannot be parsed, raises
    argparse.ArgumentTypeError.

    >>> import datetime
    >>> delta_convert(datetime.timedelta(seconds=10))
    datetime.timedelta(0, 10)

    >>> print delta_convert(None)
    None

    Time delta strings of the form #w#d#h#m#s (i.e. 2w1d5h for 2 weeks
    + 1 day + 5 hours).

    Negative signs can be used for negative deltas. If one negative
    sign is used as the first character with no other negative signs,
    then the whole expression is treated as a negative. Otherwise,
    negatives are per each time bin type.

    >>> delta_str = '3w4h'
    >>> delta_convert(delta_str)
    datetime.timedelta(21, 14400)

    >>> delta_str = '-3d10m2s'
    >>> delta_convert(delta_str)
    datetime.timedelta(-4, 85798)

    >>> delta_str = '3d-10m-2s'
    >>> delta_convert(delta_str)
    datetime.timedelta(2, 85798)

    >>> delta_str = '3d-10m2s'
    >>> delta_convert(delta_str)
    datetime.timedelta(2, 85802)

    '''
    if type(dobject) is timedelta:
        return dobject

    if dobject is None:
        return dobject

    not_parseable = argparse.ArgumentTypeError(
            'Time delta string ({}) not parseable'.format(dobject)
        )

    dtype_re = re.compile(r'^(-?\d+[smhdw])+$')

    if (not isinstance(dobject,basestring) or (not dtype_re.search(dobject))):
        raise not_parseable

    dtypes = ['seconds','minutes','hours','days','weeks']
    deltas = {t:sum(int(v) for v in re.findall(r"(-?\d+){}".format(t[0]),
                                               dobject))
              for t in dtypes}
    
    if dobject[0]=='-' and dobject.count('-')==1:
        deltas = {k:-abs(v) for k,v in deltas.items()}

    return timedelta(**deltas)
        

def odd_date_formats():
    ''' Non-standard date_formats that dateutil.parser mishandles
    '''
    return ['%Y-%m-%d-%H-%M-%S','%Y-%m-%d-%H-%M','%Y_%m_%d']

def date_convert(dobject):
    '''String to datetime/timedelta transform type for arguments

    If None, a timedelta or datetime object is passed in, it is
    returned. Time delta strings (see below) are converted to
    timedelta objects, and standard timestamps are converted to
    dateteime objects. If the string cannot be parsed, raises
    argparse.ArgumentTypeError.

    # None is returned back
    >>> print date_convert(None)
    None

    # datetime and timedeltas are no-op'ed and returned back
    
    >>> import datetime
    >>> date_convert(datetime.datetime(2012,1,1))
    datetime.datetime(2012, 1, 1)
    >>> date_convert(datetime.timedelta(seconds=10))
    datetime.timedelta(0, 10)

    # Non-standard timestamps

    >>> ts = '2013-03-05-12-45-25'
    >>> date_convert(ts)
    datetime.datetime(2013, 3, 5, 12, 45, 25)

    # Time delta strings are parsed (see delta_convert above)

    # Standard timestamps

    >>> ts = '2013-03-05 12:45:25'
    >>> date_convert(ts)
    datetime.datetime(2013, 3, 5, 12, 45)

    '''
    if isinstance(dobject, (datetime,timedelta)):
        return dobject
    elif isinstance(dobject,(date,)):
        return datetime.combine(dobject,datetime.min.time())

    if dobject is None:
        return dobject

    errors = False

    # Test the odd date formats
    for fmt in odd_date_formats():
        try:
            dt = datetime.strptime(dobject,fmt)
            return dt
        except ValueError:
            pass

    # Try to parse as time delta 
    try:
        delta = delta_convert(dobject)
        if type(delta) is timedelta:
            return delta
    except argparse.ArgumentTypeError:
        errors = True

    # Try to parse datetime using dateutil.parser
    try:
        dt = dateutil.parser.parse(dobject,ignoretz=True,fuzzy=True)
        return dt
    except ValueError:
        errors = True

    if errors:
        raise argparse.ArgumentTypeError(
            'Timestamp string ({}) not parseable'.format(dobject)
        )

def parse_times(start,end):
    '''Given arguments start and end, return two datetime objects

    start and end can be given as timestamp/delta strings

    If start and end are both datetimes, no-op, return both back.

    If start and end are None, no-op, return both back.

    Otherwise:

    If start is a datetime and end is a timedelta, convert end to
    start + delta. Else if end is a datetime and start is a timedelta,
    convert start to end - delta. Else if end is None and start is a
    timedelta, end is now and start is end - delta.

    Otherwise, raise argparse.ArgumentTypeError

    '''
    cant_parse = argparse.ArgumentTypeError(
        'Cannot parse start ({start}) and end {end}'.format(
            start=start,end=end
        )
    )

    start = date_convert(start)
    end = date_convert(end)
    
    if (type(start) is datetime) and (type(end) is datetime):
        pass
    elif start is None and end is None:
        pass
    else:
        if (type(start) is datetime) and (type(end) is timedelta):
            end = start + end
        elif (type(end) is datetime) and (type(start) is timedelta):
            start = end - start
        elif (end is None) and (type(start) is timedelta):
            end = datetime.now()
            start = end - start
        else:
            raise cant_parse
    return start,end
