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
from __future__ import absolute_import
try:
    import simplejson as _json
except ImportError:
    import json as _json
import ast
import collections
from copy import deepcopy
from datetime import datetime

from scape.utils.log import new_log
from scape.utils.file import zip_open

__all__ = ['json_dict', 'ScapeJsonReadError',
           'read_json_data', 'read_json', 'read_ndjson']

_log = new_log('scape.utils.json')

def json_dict(D,ts_format='%Y-%m-%d %H:%M:%S'):
    '''Given a dictionary D, return a "JSON-friendly" version of this
    dictionary.

    This basically means 1) converting non-string keys to string keys,
    2) converting sets and tuples to lists and datetimes to string
    timestamps (whose formats can be given in ts_format). After
    coercing sets and tuples to lists, these lists are then traversed
    and their values are converted (possibly traversed as well).

    >>> json_dict({'ts': datetime.datetime(2014,10,5,12)})
    {'ts': '2014-10-05 12:00:00'}
    >>> json_dict({'a': [{1,2,3}, 'b', (1,2)]})
    {'a': [[1,2,3], 'b', [1,2]]}

    '''
    new = deepcopy(D)

    stack = [new]

    while stack:
        n = stack.pop()

        if isinstance(n,list):
            for i,value in enumerate(n):
                if isinstance(value,datetime):
                    n[i] = str(value)

                if isinstance(value, (set,tuple)):
                    n[i] = list(value)
                elif isinstance(value,dict):
                    stack.append(dict(value))
                    
                if isinstance(value, list):
                    stack.append(value)

        elif isinstance(n,dict):
            for key,value in n.items():
                if not isinstance(key, basestring):
                    key = str(key)
                    del n[key]
                    n[key] = value

                # convert datetime objects to timestamps
                if isinstance(value,datetime):
                    n[key] = value.strftime(ts_format)

                # convert sets and tuples to lists
                if isinstance(value,(set,tuple)):
                    n[key] = list(value)

                # travese lists and dictionaries
                if isinstance(value,(list,dict)):
                    stack.append(value)

    return new

class ScapeJsonReadError(Exception):
    pass

def literal_eval(node_or_string):
    """Modified version of ast.literal_eval for parsing Python-inflected
    pseudo-JSON

    Safely evaluate an expression node or a string containing a Python
    expression.  The string or node provided may only consist of the
    following Python literal structures: strings, numbers, tuples,
    lists, dicts, Python booleans (True/False), JSON booleans
    (true/false), null and None.

    dicts are parsed as collections.OrderDict objects (to preserve
    ordering if required)

    """
    _safe_names = {
        'None': None, 'null': None,
        'True': True, 'true': True,
        'False': False, 'false': False,
    }

    if isinstance(node_or_string, basestring):
        try:
            node_or_string = ast.parse(node_or_string, mode='eval')
        except Exception as e:
            raise ScapeJsonReadError(e)

    if isinstance(node_or_string, ast.Expression):
        node_or_string = node_or_string.body

    def _convert(node):
        if isinstance(node, ast.Str):
            return node.s
        elif isinstance(node, ast.Num):
            return node.n
        elif isinstance(node, ast.Tuple):
            return list(map(_convert, node.elts))
        elif isinstance(node, ast.List):
            return list(map(_convert, node.elts))
        elif isinstance(node, ast.Dict):
            return collections.OrderedDict(
                (_convert(k), _convert(v))
                for k, v in zip(node.keys, node.values)
            )
        elif isinstance(node, ast.Name):
            if node.id in _safe_names:
                return _safe_names[node.id]
        elif isinstance(node, ast.BinOp) and \
             isinstance(node.op, (ast.Add, ast.Sub)) and \
             isinstance(node.right, ast.Num) and \
             isinstance(node.right.n, complex) and \
             isinstance(node.left, ast.Num) and \
             isinstance(node.left.n, (int, long, float)):
            left = node.left.n
            right = node.right.n
            if isinstance(node.op, ast.Add):
                return left + right
            else:
                return left - right
        raise ScapeJsonReadError('malformed string')

    return _convert(node_or_string)

def read_json_data(data):
    '''A more forgiving JSON reader.

    First tries the scape.utils.json.literal_eval function. If that
    fails, then tries the actual JSON parser. If that fails, then
    raises ScapeJsonReadError

    CAVEAT EMPTOR: This function's primary purpose is to allow some
    leeway for Pythonic idioms when typing/editing JSON (e.g. using
    single quotes, using tuples instead of lists/arrays, having
    trailing commas in sequences/objects). It is not meant to be a
    merge of Python and JSON datatypes.

    '''
    data = data.strip()
    try:
        obj = literal_eval(data)
    except ScapeJsonReadError:
        try:
            obj = _json.loads(data)
        except Exception as e:
            _log.error('Cannot parse JSON data.')
            raise ScapeJsonReadError(e)
    return obj
    
def read_json(path):
    '''Parses (possibly compressed) JSON-encoded file at path and returns
    the encoded object

    Uses a relaxed JSON standard (with certain caveats), see
    read_json_data above

    If the path ends in .gz or .bz2 then scape.utils.file.zip_open
    uses the corresponding decompression method to read the file.

    '''
    with zip_open(path) as rfp:
        data = rfp.read()

    try:
        obj = read_json_data(data)
    except:
        _log.error('\nCannot parse JSON path: {}'.format(path))
        raise

    return obj

    
def read_ndjson(path):
    '''Parses (possibly compressed) newline-delimited JSON file at path
    and returns a generator for the encoded object list

    Uses a relaxed JSON standard (with certain caveats), see
    read_json_data above

    If the path ends in .gz or .bz2 then scape.utils.file.zip_open
    uses the corresponding decompression method to read the file.

    '''
    with zip_open(path) as rfp:
        for line in rfp:
            try:
                yield read_json_data(line)
            except:
                _log.error('\nCannot parse JSON path: {}'.format(path))
                raise
    

