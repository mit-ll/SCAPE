# Copyright (2016) Massachusetts Institute of Technology.  Reproduction/Use 
# of all or any part of this material shall acknowledge the MIT Lincoln 
# Laboratory as the source under the sponsorship of the US Air Force 
# Contract No. FA8721-05-C-0002.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''scape.utils.decorators

Utility decorators for SCAPE

'''
import functools
import types

from scape.utils.log import new_log

_log = new_log('scape.utils.decorator')

def remove_nulls(*null_values):
    '''Decorator for any dictionary-returning function that removes keys
    whose values 1) evaluate to false or 2) are provided as positional
    arguments

    >>> @remove_nulls('x')
    ... def test():
    ...     return {'key0': 'x', 'key1': [], 'key2': 'a'}
    ...
    >>> test()
    {'key2': 'a'}

    ''' 
    null_values = set(null_values)
    def null_remover(f):
        def null(*a,**kw):
            D = f(*a,**kw)
            for k,v in list(D.items()):
                if not v:
                    del D[k]
                elif v in null_values:
                    del D[k]
            return D
        return null
    return null_remover

def memoized(func):
    '''Decorator for functions that memoizes the returned value

    >>> @memoized
    ... def expensive_calculation(line):
    ...     return int(line.strip())
    >>>

    '''
    attr_name = '_{0}_value'.format(func.__name__)

    @functools.wraps(func)
    def func_memoized(*a, **kw):
        if not hasattr(func, attr_name):
            setattr(func, attr_name, func(*a, **kw))
        return getattr(func, attr_name)

    return func_memoized
    

def memoized_property(fget):
    '''Decorator for instance properties that memoizes the returned value

    >>> def expensive_calculation(line):
    ...     return int(line.strip())
    >>>
    >>> class X(object):
    ...     times_called = 0
    ...     @memoized_property
    ...     def lazy_sum(self):
    ...         self.times_called += 1
    ...         with open('really_big_file.dat') as rfp:
    ...             values = []
    ...             for line in rfp:
    ...                 values.append(expensive_calculation(line))
    ...         return sum(values)
    ...
    >>> x = X()
    >>> x.times_called
    0
    >>> x.lazy_sum
    2343029023420L
    >>> x.times_called
    1
    >>> x.lazy_sum
    2343029023420L
    >>> x.times_called
    1
    >>>

    '''
    attr_name = '_{0}'.format(fget.__name__)

    @functools.wraps(fget)
    def fget_memoized(self):
        if (not hasattr(self, attr_name)) or (getattr(self,attr_name) is None):
            setattr(self,attr_name,fget(self))
        return getattr(self, attr_name)

    return property(fget_memoized)
    

def watch_property(*attributes):
    '''Decorator for instance properties that memoizes the returned value
    and refreshes the memoized value if the any of the given
    attributes have changed. To force a reset of a property "prop_x",
    a touch method, "prop_x_touch()" is added.

    >>> def expensive_calculation(line):
    ...     return int(line.strip())
    >>>
    >>> class X(object):
    ...     times_called = 0
    ...     path = 'really_big_file.dat'
    ...     @watch_property('path')
    ...     def lazy_sum(self):
    ...         self.times_called += 1
    ...         with open(self.path) as rfp:
    ...             values = []
    ...             for line in rfp:
    ...                 values.append(expensive_calculation(line))
    ...         return sum(values)
    ...
    >>> x = X()
    >>> x.times_called
    0
    >>> x.lazy_sum
    2343029023420L
    >>> x.times_called
    1
    >>> x.lazy_sum
    2343029023420L
    >>> x.times_called
    1
    >>>
    >>> x.path = 'different_really_big_file.dat'
    >>> x.lazy_sum
    7303940198409032L
    >>> x.times_called
    2
    >>> x.lazy_sum
    7303940198409032L
    >>> x.times_called
    2
    >>>
    >>> x.lazy_sum_touch()
    >>> x.lazy_sum
    7303940198409032L
    >>> x.times_called
    3
    >>>

    '''
    def watcher(fget):
        attr_name = '_{0}'.format(fget.__name__)
        touch_method = '{0}_touch'.format(fget.__name__)

        needs_reset = '{0}_needs_reset'.format(attr_name)
        watch_list = '{0}_attrs'.format(attr_name)

        @functools.wraps(fget)
        def fget_memoized(self):
            if (not hasattr(self, watch_list)):
                setattr(self,watch_list,
                        {a:getattr(self,a) for a in attributes})
                setattr(self,needs_reset,True)
                def touch(self):
                    setattr(self,needs_reset,True)
                setattr(self,touch_method,
                        types.MethodType(touch, self, self.__class__))

            watch_lut = getattr(self,watch_list)

            for a in attributes:
                if watch_lut[a] != getattr(self,a):
                    watch_lut[a] = getattr(self,a)
                    setattr(self,needs_reset,True)

            if ((not hasattr(self, attr_name)) or getattr(self,needs_reset)):
                setattr(self,attr_name,fget(self))
                setattr(self,needs_reset,False)

            return getattr(self, attr_name)

        return property(fget_memoized)
    return watcher

def singleton(func):
    '''Decorator for building memoized functions (as opposed to memoized
    class methods)

    >>> @singleton
    ... def expensive_function():
    ...     print 'this is expensive'
    ...     return reduce(lambda x,y: x*y, range(1,20))
    ...
    >>> expensive_function()
    this is expensive
    121645100408832000
    >>> expensive_function()
    121645100408832000

    '''
    func._value = None
    def wrap(*a,**kw):
        if func._value is None:
            value = func(*a,**kw)
            func._value = value
        return func._value
    return wrap
