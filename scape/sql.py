from __future__ import absolute_import
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
from .registry.parsing import parse_binary_condition, parse_list_fieldselectors


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
        new_text = '(' + sep.join(texts) + ')'
        new_params = _merge_dicts(*params)
        return new_text, new_params


class SqlSelect(scape.registry.Select):
    '''
    '''
    def __init__(self, data_source, fields=None, condition=None, **ds_kwargs):
        super(SqlSelect, self).__init__(data_source, fields, condition, **ds_kwargs)

    def _create(self, data_source, fields=None, condition=None, **ds_kwargs):
        '''Method to create a new selection of the same type as the
        original. Must be overriden in subclasses and used for
        instantiation in this class.
        '''
        return SqlSelect(data_source, fields, condition, **ds_kwargs)

    def pandas(self, **kw_args):
        kw_args['out'] = 'pandas'
        return self._data_source.run(self, **kw_args)

    def list(self, **kw_args):
        kw_args['out'] = 'list'
        return self._data_source.run(self, **kw_args)

    def iter(self, **kw_args):
        kw_args['out'] = 'iter'
        return self._data_source.run(self, **kw_args)


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

          Tuple[str, Dict[str, Any]]: Tuple of SELECT statement (str)
            and value parameters (Dict[str, Any])

        '''
        condition = self._rewrite(select.condition)

        text, params = _condition_to_where(condition)
        # potential SQL injection in field_names
        fields = sorted(self._field_names(select))
        nresults = select._ds_kwargs['limit'] if 'limit' in select._ds_kwargs else None
        statement = (
            "SELECT {fields} FROM {table} {where} {limit}".format(
                fields=','.join(fields) if fields else '*',
                table=self._table,
                where='WHERE {}'.format(text) if text else '',
                limit='LIMIT {}'.format(nresults) if nresults else '',
            )
        ).strip()

        _log.debug('sql statement: %s',statement)
        _log.debug('sql params: %s',params)

        return statement, params

    def select(self, fields='*', condition=None, **ds_args):
        fields = parse_list_fieldselectors(fields)
        return SqlSelect(self, fields, condition, **ds_args)

    def run(self, select, **kw_args):
        '''run the selection operation

        Returns:

          DataFrameResults: result of SQL selection as `DataFrame`
           object wrapped in a metadata-aware `DataFrameResults`
           object

        '''
        statement, params = self._generate_statement(select)

        text = sqlalchemy.text(statement)
	
        select_fields = set(self._field_names(select))
        all_fields = set(self.all_field_names)
        datetime_fields = (
            set(self.get_field_names('datetime')) &
            (select_fields if select_fields else all_fields)
        )

        out = kw_args.get('out', 'pandas')

        df = pandas.read_sql(text, self._engine, params=params,
                                  parse_dates=datetime_fields)

        if out == 'pandas':
            return df
        elif out == 'iter':
            return df.to_dict(orient='record')
        elif out == 'list':
            return df.to_dict(orient='record')
        else:
            raise ValueError('Unknown output format: {}'.format(out))
