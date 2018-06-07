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
import pandas.util.testing as ptesting

import scape.registry as registry
import scape.sql as sql
from scape.registry.parsing import parse_binary_condition


_log = logging.getLogger('test_sql')
_log.addHandler(logging.NullHandler())

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)

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
                parse_binary_condition('@raw_column == 5')
            ),
            ('(raw_column = :param_raw_column_0)', {'param_raw_column_0':5})
        )
    def test_condition_to_where_string(self):
        sql._ParamCreator.index = 0
        self.assertEqual(
            sql._condition_to_where(
                parse_binary_condition('@raw_column == "test"')
            ),
            ('(raw_column = :param_raw_column_0)',
             {'param_raw_column_0':'test'})
        )
    def test_condition_to_where_string_wc(self):
        sql._ParamCreator.index = 0

        self.assertEqual(
            sql._condition_to_where(
                parse_binary_condition('@raw_column == "test*"')
            ),
            ('(raw_column LIKE :param_raw_column_0)',
             {'param_raw_column_0':'test%'})
        )

        self.assertEqual(
            sql._condition_to_where(
                parse_binary_condition('@raw_column == "*test*"')
            ),
            ('(raw_column LIKE :param_raw_column_1)',
             {'param_raw_column_1':'%test%'})
        )

class TestSqlDataSource(unittest.TestCase):
    def setUp(self):
        self.engine = sqlalchemy.create_engine('sqlite:///:memory:', echo=True)
        start = datetime.datetime(2016,6,20,10)
        delta = datetime.timedelta(minutes=15)
        rows = [
            (start+delta*0, '192.168.1.1', '10.0.0.5', 32, 4096 ),
            (start+delta*1, '192.168.1.1', '10.0.0.3', 64, 2048 ),
            (start+delta*2, '192.168.1.10', '10.0.0.5', 128, 1024 ),
            (start+delta*3, '192.168.3.23', '10.0.0.10', 256, 512 ),
            (start+delta*4, '192.168.1.1', '10.0.0.5', 512, 256 ),
            (start+delta*5, '192.168.8.8', '10.0.1.1', 1024, 128 ),
            (start+delta*6, '192.168.2.2', '10.0.10.93', 2048, 64 ),
            (start+delta*7, '192.168.3.23', '10.0.10.92', 4096, 32 ),
        ]
        columns = ['time','dst_ip','src_ip','dst_bytes', 'src_bytes']
        self.df = pandas.DataFrame(rows, columns=columns)
        self.table_name = 'test'
        self.description = 'test in-memory sqlite3 table'
        self.df.to_sql(self.table_name,self.engine, index=None)

        self.metadata = registry.TableMetadata({
            'time': {'dim': 'datetime'},
            'dst_ip': {'dim': 'ip', 'tags': ['dest'], },
            'src_ip': {'dim': 'ip', 'tags': ['source'], },
            'src_bytes': {'dim': 'bytes', 'tags': ['source'], },
            'dst_bytes': {'dim': 'bytes', 'tags': ['dest'], },
        })

    def test_creation(self):
        sqlds = sql.SqlDataSource(
            engine=self.engine, metadata=self.metadata,
            table=self.table_name, description=self.description,
        )
        self.assertEqual(sqlds._engine, self.engine)
        self.assertEqual(sqlds.description, self.description)
        self.assertEqual(sqlds._table, self.table_name)
        self.assertEqual(sqlds.metadata, self.metadata)

    def test_generate_statement_no_where_empty_star(self):
        sqlds = sql.SqlDataSource(
            engine=self.engine, metadata=self.metadata,
            table=self.table_name, description=self.description,
        )

        empty_select = sqlds.select()
        star_select = sqlds.select('*')
        
        self.assertEqual(
            sqlds._generate_statement(empty_select),
            ('SELECT * FROM test', {})
        )
        self.assertEqual(
            sqlds._generate_statement(star_select),
            ('SELECT * FROM test', {})
        )
        self.assertEqual(
            sqlds._generate_statement(empty_select),
            sqlds._generate_statement(star_select),
        )

    def data_source(self):
        return sql.SqlDataSource(
            engine=self.engine, metadata=self.metadata,
            table=self.table_name, description=self.description,
        )

    def test_generate_statement_no_where_ip_dim(self):
        sqlds = self.data_source()

        ip_select = sqlds.select('ip')
        self.assertEqual(
            sqlds._generate_statement(ip_select),
            ('SELECT dst_ip,src_ip FROM test', {})
        )
        
    def test_generate_statement_no_where_dest_tag(self):
        sqlds = self.data_source()

        dest_select = sqlds.select('dest:')
        self.assertEqual(
            sqlds._generate_statement(dest_select),
            ('SELECT dst_bytes,dst_ip FROM test', {})
        )

    def test_generate_statement_no_where_source_tag(self):
        sqlds = self.data_source()

        source_select = sqlds.select('source:')
        self.assertEqual(
            sqlds._generate_statement(source_select),
            ('SELECT src_bytes,src_ip FROM test', {})
        )

    def test_generate_statement_with_where_empty_star(self):
        sqlds = self.data_source()

        empty_select = sqlds.select().where('ip == "192.168.1.1"')
        star_select = sqlds.select('*').where('ip == "192.168.1.1"')
        
        sql._ParamCreator.index = 0
        self.assertEqual(
            sqlds._generate_statement(empty_select),
            ('SELECT * FROM test WHERE ((dst_ip = :param_dst_ip_0)'
             ' OR (src_ip = :param_src_ip_1))',
             {'param_dst_ip_0':'192.168.1.1',
              'param_src_ip_1':'192.168.1.1',})
        )
        sql._ParamCreator.index = 0
        self.assertEqual(
            sqlds._generate_statement(star_select),
            ('SELECT * FROM test WHERE ((dst_ip = :param_dst_ip_0)'
             ' OR (src_ip = :param_src_ip_1))',
             {'param_dst_ip_0':'192.168.1.1',
              'param_src_ip_1':'192.168.1.1',})
        )
        

    def test_generate_statement_with_where_bytes_dim(self):
        sqlds = self.data_source()

        sql._ParamCreator.index = 0
        ip_select = sqlds.select('bytes').where('ip == "192.168.1.1"')
        self.assertEqual(
            sqlds._generate_statement(ip_select),
            ('SELECT dst_bytes,src_bytes FROM test WHERE'
             ' ((dst_ip = :param_dst_ip_0)'
             ' OR (src_ip = :param_src_ip_1))',
             {'param_dst_ip_0':'192.168.1.1',
              'param_src_ip_1':'192.168.1.1',})
        )
        
    def test_generate_statement_with_where_dest_tag(self):
        sqlds = self.data_source()

        sql._ParamCreator.index = 0
        dest_select = sqlds.select('dest:').where('ip == "192.168.1.1"')
        self.assertEqual(
            sqlds._generate_statement(dest_select),
            ('SELECT dst_bytes,dst_ip FROM test WHERE'
             ' ((dst_ip = :param_dst_ip_0)'
             ' OR (src_ip = :param_src_ip_1))',
             {'param_dst_ip_0':'192.168.1.1',
              'param_src_ip_1':'192.168.1.1',})
        )

    def test_get_field_names(self):
        sqlds = self.data_source()

        self.assertEqual(
            set(sqlds.get_field_names('source:')),
            {'src_ip', 'src_bytes'}
        )

        self.assertEqual(
            set(sqlds.get_field_names('source:','dest:')),
            {'src_ip', 'src_bytes', 'dst_ip', 'dst_bytes'}
        )

        self.assertEqual(
            set(sqlds.get_field_names('ip','datetime')),
            {'src_ip', 'dst_ip', 'time'}
        )

    def test_empty_select_run_all_data(self):
        sqlds = self.data_source()

        all_df = sqlds.select().pandas()
        _log.debug(all_df.columns)
        _log.debug(all_df.dtypes)
        _log.debug(self.df.columns)
        _log.debug(self.df.dtypes)
        ptesting.assert_frame_equal( all_df, self.df )

    def test_ip_select_run_all_data(self):
        sqlds = self.data_source()

        ip_df = sqlds.select('ip').pandas()
        ptesting.assert_frame_equal(
            ip_df,
            self.df[['dst_ip','src_ip']],
        )
        

    def test_empty_select_run_with_where(self):
        sqlds = self.data_source()

        ptesting.assert_frame_equal(
            sqlds.select().where('ip == "192.168.1.1"').pandas(),
            self.df[self.df.dst_ip == '192.168.1.1'].reset_index(drop=True)
        )

        ptesting.assert_frame_equal(
            sqlds.select().where('source:ip == "10.0.0.5"').pandas(),
            self.df[self.df.src_ip == '10.0.0.5'].reset_index(drop=True)
        )
        
    def test_empty_select_run_with_where_wc(self):
        sqlds = self.data_source()

        ptesting.assert_frame_equal(
            sqlds.select().where('ip == "192.168.1.*"').pandas(),
            self.df[self.df.dst_ip.str.startswith('192.168.1')].reset_index(drop=True)
        )

        ptesting.assert_frame_equal(
            sqlds.select().where('source:ip == "*.5"').pandas(),
            self.df[self.df.src_ip.str.endswith('.5')].reset_index(drop=True)
        )
        
    def test_bytes_select_run_with_where(self):
        sqlds = self.data_source()

        ptesting.assert_frame_equal(
            sqlds.select('bytes').where('ip == "192.168.1.1"').pandas(),
            self.df[self.df.dst_ip == '192.168.1.1'].reset_index(drop=True)[['dst_bytes','src_bytes']]
        )

        ptesting.assert_frame_equal(
            sqlds.select('bytes').where('source:ip == "10.0.0.5"').pandas(),
            self.df[self.df.src_ip == '10.0.0.5'].reset_index(drop=True)[['dst_bytes','src_bytes']]
        )
        
    def test_bytes_select_run_with_where_wc(self):
        sqlds = self.data_source()

        ptesting.assert_frame_equal(
            sqlds.select('bytes').where('ip == "192.168.1.*"').pandas(),
            self.df[self.df.dst_ip.str.startswith('192.168.1')].reset_index(drop=True)[['dst_bytes','src_bytes']]
        )

        ptesting.assert_frame_equal(
            sqlds.select('bytes').where('source:ip == "*.5"').pandas(),
            self.df[self.df.src_ip.str.endswith('.5')].reset_index(drop=True)[['dst_bytes','src_bytes']]
        )
        
    def test_select_set(self):
        sqlds = self.data_source()
        ptesting.assert_frame_equal(
            sqlds.select('bytes').where('ip == {192.168.1.10, 192.168.3.23}').pandas(),
            self.df[(self.df.dst_ip == '192.168.1.10') | (self.df.dst_ip == '192.168.3.23')].reset_index(drop=True)[['dst_bytes','src_bytes']]
        )
