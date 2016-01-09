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
''' scape.utils.time

Utilities for dealing with datetimes and timestamps
'''
from __future__ import absolute_import
import time as _time
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta as rdelta

from scape.utils.log import new_log
from scape.utils.args import (
    date_convert, delta_convert, parse_times,
)

_log = new_log('scape.utils.time')

_ceil_floor_docs = ''' {utype} dt (a datetime object) based on delta (timedelta)

dt and delta can both be specified as strings

If delta >= 7 days, the first 7 days are used to {type} dt to midnight
of the previous Sunday, and the remainder of the delta is used to
{type} dt.

If delta >= 1 day, the first day {type}s dt to midnight, and the
remainder of delta is used to {type} dt.

returns datetime
'''
def dtfloor(dt,delta):
    dt = date_convert(dt)
    delta = delta_convert(delta)

    week = timedelta(days=7)
    day = timedelta(days=1)

    def floor_to_midnight(d):
        return d - timedelta(hours=d.hour,minutes=d.minute,
                              seconds=d.second,microseconds=d.microsecond)
    def floor_to_sunday(d):
        return d - timedelta(days=((d.weekday()+1)%7))

    if (delta >= week):
        # Floor to midnight
        dt = floor_to_midnight(dt)
        # Floor to the beginning of the week (Sunday)
        dt = floor_to_sunday(dt)
        # This counts as a week, so subtract a week from delta for the
        # "week" we just removed
        delta -= week
        # go ahead and floor all week periods in the delta
        while (delta>=week):
            dt -= week
            delta -= week

    elif (delta >= day):
        # Floor to the beginning of the day
        dt = floor_to_midnight(dt)
        # This counts as one "day"
        delta -= day


    # Subtract all days from datetime and delta
    while delta >= day:
        dt -= day
        delta -= day

    if delta:
        dtsecs = _time.mktime(dt.timetuple())
        secs = dtsecs-dtsecs%(delta.total_seconds())
        dt = datetime.fromtimestamp(secs)

    return dt
dtfloor.__doc__ = _ceil_floor_docs.format(type='floor',utype='Floor')


def dtceil(dt,delta):
    delta = delta_convert(delta)
    return dtfloor(dt,delta) + delta
dtceil.__doc__ = _ceil_floor_docs.format(type='ceil',utype='Ceil')

def datetime_range(start,end,delta,exact=False):
    '''Return range of (dt0,dt1) time periods of size delta between start
    and end, where start is floored and end is ceiled (see
    dtfloor/dtceil above) using delta by default

    start and end can be datetime objects or a timestamp strings (or
    those types supported by the scape.utils.args.parse_times
    function)

    delta can be a timedelta object or a delta string

    If exact is True, then neither start nor end are
    modified. Instead, the first and last time periods are truncated
    such that they are bounded by whole times determined by delta.

    >>> start = "2014-05-10 13:25:27"
    >>> end = "2014-05-10 13:28:44"
    >>> delta = '1m'
    >>> list(datetime_range(start,end,delta))
        [(datetime.datetime(2014, 5, 10, 13, 25),
          datetime.datetime(2014, 5, 10, 13, 26)),
         (datetime.datetime(2014, 5, 10, 13, 26),
          datetime.datetime(2014, 5, 10, 13, 27)),
         (datetime.datetime(2014, 5, 10, 13, 27),
          datetime.datetime(2014, 5, 10, 13, 28)),
         (datetime.datetime(2014, 5, 10, 13, 28),
          datetime.datetime(2014, 5, 10, 13, 29))]

    >>> list(datetime_range(start,end,delta,exact=True))
        [(datetime.datetime(2014, 5, 10, 13, 25, 27),
          datetime.datetime(2014, 5, 10, 13, 26)),
         (datetime.datetime(2014, 5, 10, 13, 26),
          datetime.datetime(2014, 5, 10, 13, 27)),
         (datetime.datetime(2014, 5, 10, 13, 27),
          datetime.datetime(2014, 5, 10, 13, 28)),
         (datetime.datetime(2014, 5, 10, 13, 28),
          datetime.datetime(2014, 5, 10, 13, 28, 44))]

    '''
    delta = delta_convert(delta)
    start, end = parse_times(start,end)

    if exact:
        sdt = dtfloor(start,delta) + delta
        # yield the first truncated period bounded by the given start
        # time and the first whole time boundary determined by delta
        yield (start,sdt)
        edt = dtfloor(end,delta)
    else:
        sdt = dtfloor(start,delta)
        edt = dtceil(end,delta)

    t = sdt
    while t<edt:
        tpd = t+delta
        yield (t,tpd)
        t = tpd

    if exact:
        # yield the final truncated period bounded by the last whole
        # time boundary determined by delta and the given end time
        yield (edt,end)


MINUTE,HOUR,DAY,MONTH,YEAR = (
    rdelta(minutes=1), rdelta(hours=1), rdelta(days=1),
    rdelta(months=1), rdelta(years=1),
)
delta_fmt_map = {
    id(MINUTE): '%Y%m%d%H%M',
    id(HOUR): '%Y%m%d%H',
    id(DAY): '%Y%m%d',
    id(MONTH): '%Y%m',
    id(YEAR): '%Y',
}
str_to_rdelta = {
    'minute': MINUTE,
    'hour': HOUR,
    'day': DAY,
    'month': MONTH,
    'year': YEAR,
}
def get_rdelta(size):
    if isinstance(size,rdelta):
        return size

    match = []
    if size not in str_to_rdelta:
        for k in sorted(str_to_rdelta):
            if k.startswith(size):
                match.append(k)
    else:
        match = [size]
    match.sort()
    if len(match) > 1:
        _log.warn('Ambiguous time delta: "{}", matches "{}"'.format(
            size, ', '.join(match)
        ))
    elif not match:
        raise KeyError(
            'Bad time delta: "{}", available options'
            ' are: "{}"'.format(
                size,
                ', '.join(['year','month','day','hour','minute'])
            )
        )
    return str_to_rdelta[match[0]]

def is_next_hour(dt0,dt1):
    '''Is dt1 within the next hour after dt0's hour?'''
    return rdelta(datetime(dt1.year,dt1.month,dt1.day,dt1.hour),
                  datetime(dt0.year,dt0.month,dt0.day,dt0.hour)) == HOUR
def is_next_day(dt0,dt1):
    '''Is dt1 within the next day after dt0's day?'''
    return rdelta(datetime(dt1.year,dt1.month,dt1.day),
                  datetime(dt0.year,dt0.month,dt0.day)) == DAY
def is_next_month(dt0,dt1):
    '''Is dt1 within the next month after dt0's month?'''
    return rdelta(datetime(dt1.year,dt1.month,1),
                  datetime(dt0.year,dt0.month,1)) == MONTH
def is_next_year(dt0,dt1):
    '''Is dt1 within the next year after dt0's year?'''
    return rdelta(datetime(dt1.year,1,1),datetime(dt0.year,1,1)) == YEAR

def rdt_to_secs(dt,rd):
    '''Covert relative delta object (rd) to number of seconds based on
    datetime object (dt)

    returns integer number of seconds

    '''
    return ((dt+rd)-dt).total_seconds()

def timestamp_buckets_of_max_size(start,end):
    '''Create list of timestamps from start to end which encode the whole
    time period (i.e. bucket) they express (e.g. "201401" is the whole
    month of January 2014 and "20140101" is the whole day of Jan. 1,
    2014) where each time period is as large as possible (max size of
    a year).

    >>> timestamp_buckets_of_max_size('2014-05-10 13:57','2014-05-11 03:05')
    ['201405101357',
     '201405101358',
     '201405101359',
     '2014051014',
     '2014051015',
     '2014051016',
     '2014051017',
     '2014051018',
     '2014051019',
     '2014051020',
     '2014051021',
     '2014051022',
     '2014051023',
     '2014051100',
     '2014051101',
     '2014051102',
     '201405110300',
     '201405110301',
     '201405110302',
     '201405110303',
     '201405110304',
     '201405110305']

    '''
    start,end = parse_times(start,end)
    delta_dt = MINUTE
    delta_fmt = delta_fmt_map[id(delta_dt)]

    start_dt = datetime(start.year,start.month,start.day,start.hour,start.minute)

    end_dt = datetime(end.year,end.month,end.day,end.hour,end.minute)
    end_dt = end_dt + delta_dt

    tstamps = []

    t = start_dt
    tend = end_dt

    while t<tend:
        ndt = t+delta_dt
        delta_fmt = delta_fmt_map[id(delta_dt)]
        if ndt+delta_dt <= tend:
            if is_next_year(t,ndt):
                if ndt+YEAR < tend:
                    delta_dt = YEAR
            elif is_next_month(t,ndt):
                if ndt+MONTH < tend:
                    delta_dt = MONTH
            elif is_next_day(t,ndt):
                if ndt+DAY < tend:
                    delta_dt = DAY
            elif is_next_hour(t,ndt):
                if ndt+HOUR < tend:
                    delta_dt = HOUR
        else:
            if ndt+MONTH < tend:
                delta_dt = MONTH
            elif ndt+DAY < tend:
                delta_dt = DAY
            elif ndt+HOUR < tend:
                delta_dt = HOUR
            else:
                delta_dt = MINUTE
        tstamps.append(t.strftime(delta_fmt))
        t = ndt

    return tstamps

def timestamp_buckets_of_size(start,end,size):
    '''Create list of timestamps from start to end which encode the whole
    time period (i.e. bucket) they express (e.g. "201401" is the whole
    month of January 2014 and "20140101" is the whole day of Jan. 1,
    2014) of given static size

    >>> timestamp_buckets_of_size('2014-05-10 13:25','10d','day')
    ['20140510',
     '20140511',
     '20140512',
     '20140513',
     '20140514',
     '20140515',
     '20140516',
     '20140517',
     '20140518',
     '20140519',
     '20140520']    
    '''
    start,end = parse_times(start,end)
    delta_dt = get_rdelta(size)
    delta_fmt = delta_fmt_map[id(delta_dt)]

    dts = datetime_buckets_of_size(start,end,size)

    return [t.strftime(delta_fmt) for t in dts]

def datetime_buckets_of_size(start,end,size):
    '''Create list of datetime objects from start to end where each is
    floored to the given static size

    >>> datetime_buckets_of_size(start,'10d','day')
    [datetime.datetime(2014, 5, 10, 0, 0),
     datetime.datetime(2014, 5, 11, 0, 0),
     datetime.datetime(2014, 5, 12, 0, 0),
     datetime.datetime(2014, 5, 13, 0, 0),
     datetime.datetime(2014, 5, 14, 0, 0),
     datetime.datetime(2014, 5, 15, 0, 0),
     datetime.datetime(2014, 5, 16, 0, 0),
     datetime.datetime(2014, 5, 17, 0, 0),
     datetime.datetime(2014, 5, 18, 0, 0),
     datetime.datetime(2014, 5, 19, 0, 0),
     datetime.datetime(2014, 5, 20, 0, 0)]


    '''
    start,end = parse_times(start,end)
    delta_dt = get_rdelta(size)

    if delta_dt is MINUTE:
        start_dt = datetime(start.year,start.month,start.day,start.hour,start.minute)
    elif delta_dt is HOUR:
        start_dt = datetime(start.year,start.month,start.day,start.hour)
    elif delta_dt is DAY:
        start_dt = datetime(start.year,start.month,start.day)
    elif delta_dt is MONTH:
        start_dt = datetime(start.year,start.month,1)
    elif delta_dt is YEAR:
        start_dt = datetime(start.year,1,1)

    if delta_dt is MINUTE:
        end_dt = datetime(end.year,end.month,end.day,end.hour,end.minute)
    elif delta_dt is HOUR:
        end_dt = datetime(end.year,end.month,end.day,end.hour)
    elif delta_dt is DAY:
        end_dt = datetime(end.year,end.month,end.day)
    elif delta_dt is MONTH:
        end_dt = datetime(end.year,end.month,1)
    elif delta_dt is YEAR:
        end_dt = datetime(end.year,1,1)
    end_dt = end_dt + delta_dt

    dts = []

    t = start_dt
    tend = end_dt

    while t<tend:
        ndt = t+delta_dt
        dts.append(t)
        t = ndt

    return dts

