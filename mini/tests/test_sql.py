import os
import base64
import sys
import random
import string
import logging
import pprint

if sys.version_info[:2] > (2, 7):
    # Python 3.X
    from unittest.mock import patch
    from urllib.parse import parse_qs, parse_qsl, urlparse, unquote_plus
elif sys.version_info[:2] == (2 , 7):
    # Python 2.7
    from mock import patch
    from urllib import unquote_plus 
    from urlparse import parse_qs, parse_qsl, urlparse
else:
    raise ImportError('cannot run on Python < 2.7')

import unittest

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

    def test_has_replace_escaped_wildcard(self):
        self.assertEqual(
            sql._replace_escaped_wildcard(r'\*\*test\*\*'),
            '**test**',
        )
        self.assertEqual(
            sql._replace_escaped_wildcard('\*\*test\*\*'),
            '**test**',
        )
        self.assertEqual(
            sql._replace_escaped_wildcard(r'**test**'),
            '**test**',
        )
        self.assertEqual(
            sql._replace_escaped_wildcard(r'test'),
            'test',
        )



