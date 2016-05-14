from __future__ import print_function
import os
import sys
import time
import json
import sqlite3
import logging
import re

import pandas as pd
import sqlalchemy

import scape.registry

_log = logging.getLogger('scape.sqlite') # pylint: disable=invalid-name
_log.addHandler(logging.NullHandler())

def _get_splunk_params(select):
    attrs = ['earliest', 'earliest_time',
             'index_earliest', 'index_latest',
             'latest', 'latest_time',
             'max_count', 'max_time',
             'status_buckets',
             'timeout']
    kwargs = {}
    for attr in dir(select):
        if attr in attrs:
            kwargs[attr] = getattr(select, attr)
    return kwargs

def _paren(args, sep):
    if len(args) == 1:
        return args[0]
    else:
        sep = ' {} '.format(sep)
        return '(' + sep.join(args) + ')'

_WC_ESCAPED_RE = re.compile(r'\\\*')
_WC_RE = re.compile(r'(?<!\\)\*')

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

    >>> _replace_escaped_wildcard(r'\*\*asdf\*\*') == '**asdf**'
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

    >>> _has_wildcard('*asdf*')
    True
    >>> _has_wildcard('\\*asdf\\*')
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

    >>> _replace_wildcard('*asdf*') == '%asdf%'
    True
    '''
    return _WC_RE.sub(repl, value)

def _condition_to_where(condition):
    '''Convert Condition object to a SQL WHERE clause representation

    Args:
      condition (Condition): :class:`Condition` to convert

    Returns:
      str: string WHERE clause representation of the :class:`Condition`

    '''
    value = ''

    if isinstance(condition, scape.registry.Equals):
        lhs, rhs = condition.lhs.name, condition.rhs
        operator = '='

        if _has_wildcard(rhs):
            # This should be a LIKE comparison
            operator = 'LIKE'
            rhs = _replace_wildcard(rhs)

        if _has_escaped_wildcard(rhs):
            rhs = _replace_escaped_wildcard(rhs)

        value = '({} {} "{}")'.format(lhs, operator, rhs)

    elif isinstance(condition, scape.registry.Or):
        value = _paren([_condition_to_where(x) for x in condition.xs], 'OR')

    elif isinstance(condition, scape.registry.And):
        value = _paren([_condition_to_where(x) for x in condition.xs], 'AND')

    return value


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
    >>> df = sqldata['auth'].select('source:host==C149*').run()
    '''

    def __init__(self, engine, metadata, table, description=""):
        super(SqlDataSource, self).__init__(metadata, description, {
            '==': scape.registry.Equals,
            '=~': scape.registry.MatchesCond,
        })
        self._engine = engine
        self._table = table

    def _get_field_names(self, select):
        names = set()
        for selector in select._fields:
            names.update(f.name for f in self._metadata.fields_matching(selector))
            _log.debug(names)
        return sorted(names)
    
    def generate_statement(self, select):
        '''Given Select object, generate SELECT statement as SQLAlchemy text
        clause

        Args:

          select (scape.registry.Select): Select object to translate

        Returns:

          sqlalchemy.sql.elements.TextClause: text clause for this Select

        '''
        condition = self._rewrite(select._condition)
        where_clause = _condition_to_where(condition)
        fields = self._get_field_names(select)
        # XXXX FIX!!!! XXXX
        # Major SQL injection risk here
        # FIX FIX FIX FIX
        statement = "SELECT {} FROM {} WHERE {}".format(
            ','.join(fields) if fields else '*', self._table, where_clause,
        )
        return statement

    def run(self, select):
        ''' run
        '''
        statement = sqlalchemy.text(
            self.generate_statement(select)
        )

        return pd.read_sql(statement, self._engine)

