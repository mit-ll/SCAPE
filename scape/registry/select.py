# Copyright (2016) Massachusetts Institute of Technology.
# Reproduction/Use of all or any part of this material shall
# acknowledge the MIT Lincoln Laboratory as the source under the
# sponsorship of the US Air Force Contract No. FA8721-05-C-0002.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

from __future__ import absolute_import
import copy
from collections import namedtuple

from .condition import And, Or
from .parsing import parse_binary_condition, parse_list_fieldselectors

class Select(object):
    '''Selection of fields from rows associated with a particular
    :class:`DataSource` possibly with match conditions for rows from
    data source

    Args:

      data_source (:class:`DataSource`): Subclass of DataSource that this
        selection is associated with.

      fields (List[:class:`Field`]): List of Field objects
        (e.g. columns in the case of SQL connections) to return from
        DataSource

      condition (:class:`Condition`): 

      **ds_kwargs: keyword arguments to be passed to DataSource
        (special db connection parameters, query configurations, etc.)
        at query time

    In most cases, users should _not_ create this class
    directly. Instead, they should call the ``select`` method of the
    DataSource in question.

    '''
    def __init__(self, data_source, fields=None, condition=None, **ds_kwargs):
        fields = fields if fields else []
        condition = condition if condition else And([])

        self._data_source = data_source

        self._condition = parse_binary_condition(condition)
        self._fields = parse_list_fieldselectors(fields)

        self._ds_kwargs = copy.deepcopy(ds_kwargs)

    def __repr__(self):
        return "Select({!r}, {!r}, {!r}, {!r})".format(
            self._data_source, self._fields, self._condition, self._ds_kwargs
        )

    def copy(self):
        return self._create(self._data_source, self._fields, self._condition,
                      **self._ds_kwargs)

    def _create(self, data_source, fields=None, condition=None, **ds_kwargs):
        '''Method to create a new selection of the same type as the
        original. Must be overriden in subclasses and used for
        instantiation in this class.
        '''
        return Select(data_source, fields, condition, **ds_kwargs)


    @property
    def ds_args(self):
        ''' DataSource-specific keyword args for this selection
        '''
        return namedtuple(
            'DataSourceArgs', sorted(self._ds_kwargs.keys())
        )(**self._ds_kwargs)

    @property
    def fields(self):
        ''':class:`Field` objects associated with this Select'''
        return self._fields[:]

    @property
    def condition(self):
        ':class:`Condition` associated with this Select'
        return self._condition.copy()

    def where(self, condition=None, **kw_args):
        '''Match conditions for rows to be retured from the DataSource

        Args:

          condition (str): string representation of row match
            conditions stated in terms of fields, tags and dimensions

        Example:

            >>> select = ds.select(['source:ip','dest:'])
            >>> select192 = select.where('source:==192.168.*')
            >>> iter192 = select192.run()
            >>> list(iter192)
            [{'s_ip': '192.168.1.5', 'd_ip': '59.223.1.83',
              'd_domain': 'google.com'},
             {'s_ip': '192.168.1.10', 'd_ip': '32.2.101.205',
              'd_domain': 'facebook.com'}]
        '''
        if condition:
            condition = And([parse_binary_condition(condition), self._condition])
        else:
            condition = self._condition

        new_kwargs = copy.deepcopy(self._ds_kwargs)
        new_kwargs.update(kw_args)

        select = self._create(self._data_source, self.fields, condition, **new_kwargs)
        select.check()

        return select
    
    def with_fields(self, fields):
        return self._create(self._data_source, fields, self._condition,
                      **self._ds_kwargs)

    def check(self, **kw_args):
        return self._data_source.check_select(self, **kw_args)

    def debug(self, **kw_args):
        return self._data_source.debug_select(self, **kw_args)

    def run(self, **kw_args):
        ''' Execute a query.

        Returns a data source specific object containing the results
        '''
        return self._data_source.run(self, **kw_args)
