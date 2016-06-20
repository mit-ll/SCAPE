import sys
import logging

if sys.version_info[:2] > (2, 7):
    # Python 3.X
    from unittest.mock import (
        patch, Mock, MagicMock,
    )
elif sys.version_info[:2] == (2 , 7):
    # Python 2.7
    from mock import (
        patch, Mock, MagicMock,
    )
else:
    raise ImportError('cannot run on Python < 2.7')

import unittest
import sqlalchemy
import sqlite3
import datetime
import pandas

import scape.registry as registry
import scape.sql as sql


_log = logging.getLogger('test_sql')
_log.addHandler(logging.NullHandler())

class TestWildcardFunctions(unittest.TestCase):
    def test_has_escaped_wildcard(self):
        self.assertTrue(sql._has_escaped_wildcard('\*'))
        self.assertFalse(sql._has_escaped_wildcard('*'))
        self.assertTrue(sql._has_escaped_wildcard('*\**'))
        self.assertFalse(sql._has_escaped_wildcard('a'))
        self.assertTrue(sql._has_escaped_wildcard('a\*a'))
        self.assertFalse(sql._has_escaped_wildcard('a*a'))

    def test_has_wildcard(self):
        self.assertFalse(sql._has_wildcard('\*'))
        self.assertTrue(sql._has_wildcard('*'))
        self.assertTrue(sql._has_wildcard('*\**'))
        self.assertFalse(sql._has_wildcard('a'))
        self.assertFalse(sql._has_wildcard('a\*a'))
        self.assertTrue(sql._has_wildcard('a*a'))

    def test_replace_escaped_wildcard(self):
        self.assertEqual(
            sql._replace_escaped_wildcard('\*\*test\*\*'),
            '**test**',
        )
        self.assertEqual(
            sql._replace_escaped_wildcard('**test**'),
            '**test**',
        )
        self.assertEqual(
            sql._replace_escaped_wildcard('test'),
            'test',
        )

    def test_replace_wildcard(self):
        self.assertEqual(
            sql._replace_wildcard('test*'),
            'test%',
        )
        self.assertEqual(
            sql._replace_wildcard('*test'),
            '%test',
        )
        self.assertEqual(
            sql._replace_wildcard('*test*'),
            '%test%',
        )
        self.assertEqual(
            sql._replace_wildcard('*double*test*'),
            '%double%test%',
        )
        self.assertEqual(
            sql._replace_wildcard(r'\*test\*'),
            r'\*test\*',
        )
        self.assertEqual(
            sql._replace_wildcard('test'),
            'test',
        )
        self.assertEqual(
            sql._replace_wildcard('*test*','^'),
            '^test^',
        )

    def test_condition_to_where_int(self):
        sql._ParamCreator.index = 0
        self.assertEqual(
            sql._condition_to_where(
                registry._parse_binary_condition('@raw_column == 5')
            ),
            ('(raw_column = :param_raw_column_0)', {'param_raw_column_0':5})
        )
    def test_condition_to_where_string(self):
        sql._ParamCreator.index = 0
        self.assertEqual(
            sql._condition_to_where(
                registry._parse_binary_condition('@raw_column == "test"')
            ),
            ('(raw_column = :param_raw_column_0)',
             {'param_raw_column_0':'test'})
        )
    def test_condition_to_where_string_wc(self):
        sql._ParamCreator.index = 0

        self.assertEqual(
            sql._condition_to_where(
                registry._parse_binary_condition('@raw_column == "test*"')
            ),
            ('(raw_column LIKE :param_raw_column_0)',
             {'param_raw_column_0':'test%'})
        )

        self.assertEqual(
            sql._condition_to_where(
                registry._parse_binary_condition('@raw_column == "*test*"')
            ),
            ('(raw_column LIKE :param_raw_column_1)',
             {'param_raw_column_1':'%test%'})
        )

class TestSqlDataSource(unittest.TestCase):
    def setUp(self):
        self.engine = sqlalchemy.create_engine('sqlite:///:memory:', echo=True)
        rows = [
            ('a0', 'b0', 'c0', 3, 4.2, datetime.datetime(2016,6,20,10)),
            ('a1', 'b1', 'c1', 4, 5.2, datetime.datetime(2016,6,20,10,15)),
            ('a2', 'b2', 'c2', 5, 6.2, datetime.datetime(2016,6,20,10,30)),
            ('a3', 'b3', 'c3', 6, 7.2, datetime.datetime(2016,6,20,10,45)),
        ]
        columns = ['A','B','C','D','E','F']
        self.df = pandas.DataFrame(rows, columns=columns)
        self.table_name = 'test'
        self.description = 'test in-memory sqlite3 table'
        self.df.to_sql(self.table_name,self.engine)

        self.metadata = registry.TableMetadata({
            'F': {'dim': 'datetime'},
            'A': {'dim': 'dim_a', 'tags': ['tag0','tag1'], },
            'B': {'dim': 'dim_b', 'tags': ['tag1','tag2'], },
            'C': {'dim': 'dim_c', 'tags': ['tag2','tag3'], },
            'D': {'dim': 'dim_d', 'tags': ['tag3','tag4'], },
            'E': {'dim': 'dim_e', 'tags': ['tag4','tag5'], },
        })

    def test_creation(self):
        sqldata = sql.SqlDataSource(
            engine=self.engine, metadata=self.metadata,
            table=self.table_name, description=self.description,
        )
        self.assertEqual(sqldata._engine, self.engine)
        self.assertEqual(sqldata.description, self.description)
        self.assertEqual(sqldata._table, self.table_name)
        self.assertEqual(sqldata.metadata, self.metadata)

    def test_generate_statement(self):
        pass

