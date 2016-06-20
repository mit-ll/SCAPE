from __future__ import print_function
import os
import sys
import time
import json
import sqlite3
import logging
import collections
import re

import six

import pandas
import sqlalchemy

import scape.registry

_log = logging.getLogger('scape.sql') # pylint: disable=invalid-name
_log.addHandler(logging.NullHandler())

_WC_ESCAPED_RE = re.compile(r'\\\*')
_WC_RE = re.compile(r'(?<!\\)\*')

def _merge_dicts(*dicts):
    result = {}
    for d in dicts:
        result.update(d)
    return result

def _has_escaped_wildcard(value):
    r''' Does the string value contain any escaped wildcard chars "\*" ?

    Args:

      value (str): string to search

    Returns:

      bool: True if search has escaped wildcard, False if not
    '''
    return bool(_WC_ESCAPED_RE.search(value))

def _replace_escaped_wildcard(value):
    r''' Replace escaped wildcard characters with *

    Args:
      value (str): string to search

    Returns:
      str: string with \* replaced with *

    Example:

    >>> _replace_escaped_wildcard(r'\*\*test\*\*') == '**test**'
    True
    '''
    return _WC_ESCAPED_RE.sub('*', value)

def _has_wildcard(value):
    r''' Does the string value contain any wildcard chars "*" ?

    Args:

      value (str): string to search

    Returns:

      bool: True if search has wildcard, False if not

    Example:

    >>> _has_wildcard('*test*')
    True
    >>> _has_wildcard('\\*test\\*')
    False
    >>> _has_wildcard(r'\*test\*')
    False
    '''
    return bool(_WC_RE.search(value))

def _replace_wildcard(value, repl='%'):
    r''' Replace wildcard characters with repl value (default '%')

    Args:
      value (str): string to search
      repl (str): string to replace * with

    Returns:
      str: string with * replaced with repl

    Example:

    >>> _replace_wildcard('*test*') == '%test%'
    True
    '''
    return _WC_RE.sub(repl, value)

class _ParamCreator(object):
    index = 0
    @staticmethod
    def new(param):
        name = 'param_{}_{}'.format(param, _ParamCreator.index)
        _ParamCreator.index += 1
        return name

def _condition_to_where(condition):
    '''Convert :class:`Condition` object to a SQL WHERE clause representation

    - Must pass in a condition whose LHS has been resolved to a
      :class:`Field` object.
    - Currently only handles :class:`Equals`, :class:`And` and
      :class:`Or` conditions.

    Args:
      condition (Condition): :class:`Condition` to convert

    Returns:

      Tuple[str, Dict[str,Any]]: tuple of string WHERE clause
        (suitable for a sqlalchemy `text`) and dictionary of params
        for this `text` clause

    Examples:

    >>> num_condition = Equals(Field('num_column'), 5)
    >>> _condition_to_where(num_condition)
    ('(num_column = :param_num_column_0)', {'param_num_column_0': 5})
    >>> str_condition = Equals(Field('str_column'), "test")
    '(str_column = "test")'
    >>> wc_condition  = Equals(Field('str_column'), "*test*")
    '(str_column LIKE "%test%")'

    '''
    text = ''
    params = {}

    if ( isinstance(condition, scape.registry.Equals) or
         (isinstance(condition, scape.registry.BinaryCondition) and
         condition.op == '==') ):
        # Equals condition object
        lhs, rhs = condition.lhs.name, condition.rhs
        operator = '='

        if isinstance(rhs, six.string_types):
            # String conversions of RHS
            if _has_wildcard(rhs):
                # This should be a LIKE comparison
                operator = 'LIKE'
                rhs = _replace_wildcard(rhs)

            if _has_escaped_wildcard(rhs):
                # clear the escaping for '*' character
                rhs = _replace_escaped_wildcard(rhs)
        else:
            # Numeric value
            pass

        param = _ParamCreator.new(lhs)
        value = '({lhs} {op} :{param})'.format(
            lhs=lhs, op=operator, param=param,
        )
        text = value
        params[param] = rhs

    elif isinstance(condition, scape.registry.Or):
        text, params = _paren([_condition_to_where(x) for x in condition.parts], 'OR')

    elif isinstance(condition, scape.registry.And):
        text, params = _paren([_condition_to_where(x) for x in condition.parts], 'AND')
        
    return text, params

def _paren(args, sep):
    if len(args) == 1:
        return args[0]
    else:
        sep = ' {} '.format(sep)
        texts, params = zip(*args)
        new_text = '(' + sep.join(args) + ')'
        new_params = _merge_dicts(*params)
        return new_text, new_params


class SqlDataSource(scape.registry.DataSource):
    '''SQL Data source

    Uses SQLAlchemy and Pandas read_sql* functionality to get data
    from a SQL table of data.

    Args:

      engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine where
         data resides

      metadata (scape.registry.TableMetadata): TableMetadata object
         that describes the relationship between fields, tags and
         dimensions

      table (str): table name for this data source

      description (str): description of this data source

    Example:

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://user:pass@host:5432/databasename')
    >>> sqldata = SqlDataSource(
    ...     engine=engine,
    ...     metadata=scape.registry.TableMetadata({
    ...         'time': { 'dim': 'datetime' },
    ...         'source_computer': { 'tags': ['source'],
    ...                              'dim': 'host' },
    ...         'destination_computer': { 'tags': ['dest'],
    ...                                   'dim': 'host' },
    ...     }),
    ...     table='auth',
    ...     description='User authentication event table',
    ... )
    >>> results = sqldata.select('host').where('source:host=="C149*"').run()
    >>> results['dest:host'].series.unique()
    array(['C141', 'C1412', 'C1416', 'C1417', 'C1418'], dtype=object)
    >>> results['host'].dataframe.drop_duplicates()
      destination_computer source_computer
    0                 C141            C141
    1                C1412           C1412
    2                C1416           C1416
    3                C1417           C1417
    4                C1418           C1418

    '''

    def __init__(self, engine, metadata, table, description=""):
        super(SqlDataSource, self).__init__(metadata, description, {
            '==': scape.registry.Equals,
            # '=~': scape.registry.MatchesCond,
        })
        self._engine = engine
        self._table = table

    def _generate_statement(self, select):
        '''Given Select object, generate SELECT statement as SQLAlchemy text
        clause

        Args:

          select (Select): Select object to translate

        Returns:

          sqlalchemy.sql.elements.TextClause: text clause for this Select

        '''
        condition = self._rewrite(select.condition)

        text, params = _condition_to_where(condition)
        # potential SQL injection in field_names
        fields = self.field_names(select)

        statement = "SELECT {} FROM {} WHERE {}".format(
            ','.join(fields) if fields else '*', self._table, text,
        )
        _log.debug('sql statement: %s',statement)
        _log.debug('sql params: %s',params)
        return statement, params

    def run(self, select):
        '''run the selection operation

        Returns:

          DataFrameResults: result of SQL selection as `DataFrame`
           object wrapped in a metadata-aware `DataFrameResults`
           object

        '''
        statement, params = self._generate_statement(select)

        text = sqlalchemy.text(statement)

        # return pandas.read_sql(text, self._engine, params=params)
        return DataFrameResults(
            self, pandas.read_sql(text, self._engine, params=params)
        )

    def get_fields(self, *tdims):
        '''Given tagged dimensions, return list of field names that match

        Args:
          *tdims (*str): tagged dimensions as strings (e.g. "source:ip")

        Examples:

        >>> sqldata.get_fields('source:ip','dest:ip')
        ['src_ip', 'dst_ip']
        >>> sqldata.get_fields('ip','host')
        ['src_ip', 'dst_ip', 'src_host', 'dst_host']

        '''
        fields = set()
        for tdim in tdims:
            fields.update(
                self._metadata.fields_matching(scape.registry.tagsdim(tdim))
            )
        return [f.name for f in fields]
    
class DataFrameResults(collections.Iterator):
    '''Container for `DataFrame` results returned from
    :class:`SqlDataSource` object's `run` method

    Args:

      datasource (SqlDataSource): SqlDataSource that produced these resultsq

      dataframe (DataFrame): Pandas DataFrame produced by the
        DataSource select operation

    Examples:

    >>> results = sqldata.select().where('dest:ip == '192.168.1.1').run()
    >>> results['ip'].series
    0 192.168.1.1
    1 192.168.1.1
    2 192.168.1.1
    3 192.168.1.1
    0 10.0.0.5
    1 10.0.0.5
    2 10.0.0.3
    3 10.0.0.5
    >>> results['ip'].series.unique()
    array(['192.168.1.1','10.0.0.5','10.0.0.3'], dtype=object)
    >>> results['ip'].dataframe.drop_duplicates()
         src_ip        dst_ip
    0  10.0.0.5   192.168.1.1
    1  10.0.0.3   192.168.1.1
    >>> results['source:ip'].series.unique()
    array(['10.0.0.5','10.0.0.3'], dtype=object)
    '''
    def __init__(self, datasource, dataframe):
        self._datasource = datasource
        self.dataframe = dataframe

    def iter(self):
        return iter(self)

    _row_iter = None
    def __iter__(self):
        def dict_iter():
            cols = list(self.dataframe.columns)
            for row in self.dataframe.itertuples():
                yield {c:v for c,v in zip(cols,row)}
        if self._row_iter is None:
            self._row_iter = dict_iter()
        return self._row_iter

    def __next__(self):
        return next(self._row_iter)
    next = __next__             # Python 2.7x 

    def __getitem__(self, tdim):
        if isinstance(tdim, six.string_types):
            fields = self._datasource.get_fields(tdim)
            return DataFrameResults(
                self._datasource,
                self.dataframe[fields],
            )
            # string key
            tagsdim = scape.registry.tagsdim(tdim)
            if tagsdim.dim is None:
                # No dimension given, only tags. Assume this will
                # return multiple dimensions, so return
                # DataFrameResults with subset of (multi-dimension)
                # fields (via recursive call with list argument)
                return self[[tdim]]
            else:
                # Assume that all elements of given dimension can be
                # sensibly concatenated together into a single Pandas
                # Series
                fields = self._datasource.get_fields(tdim)
                return pandas.concat([self.dataframe[f] for f in fields])
        else:
            # sequence key
            # 
            # return pandas DataFrameResults with subset of fields
            # based on tagged dimensions
            fields = self._datasource.get_fields(*tdim)
            return DataFrameResults(
                self._datasource,
                self.dataframe[fields],
            )

    @property
    def series(self):
        ''' return DataFrame flattened into single Series object

        Examples:

        >>> results = sqldata.select().where('dest:ip == '192.168.1.1').run()
        >>> results['ip'].series
        0 192.168.1.1
        1 192.168.1.1
        2 192.168.1.1
        3 192.168.1.1
        0 10.0.0.5
        1 10.0.0.5
        2 10.0.0.3
        3 10.0.0.5
        >>> results['ip'].series.unique()
        array(['192.168.1.1','10.0.0.5','10.0.0.3'], dtype=object)
        >>> results['source:ip'].series.unique()
        array(['10.0.0.5','10.0.0.3'], dtype=object)
        '''
        columns = list(self.dataframe.columns)
        return pandas.concat([self.dataframe[c] for c in columns])

    # def drop_duplicates(self):
    #     return DataFrameResults(self._datasource,
    #                             self.dataframe.drop_duplicates())

    # def unique(self):
    #     return self.dataframe.drop_duplicates
