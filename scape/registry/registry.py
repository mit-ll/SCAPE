from __future__ import absolute_import

import os
import importlib
import logging

from ..utils import merge_dicts
from ..utils.yaml import read_yaml

from .table_metadata import TableMetadata

_log = logging.getLogger('scape.registry.registry')
_log.addHandler(logging.NullHandler())

class Registry(dict):
    '''A collection of data sources.

    Args:

      data_sources (Dict[str, :class:`DataSource`])): dictionary
        mapping data source names to DataSource objects

    Example:

        >>> registry = Registry( {
        ... })
        >>>

    '''

    def __init__(self, data_sources):
        self.update(data_sources)

    def _repr_html_(self):
        res = ['<table>']
        def td(d):
            res.extend(['<td>',d,'</td>'])
        res.append('<th>Name</th><th>Class</th><th>Description</th></tr>')
        for k in sorted(self.keys()):
            ds = self[k]
            res.append('<tr>')
            td(k)
            td(ds.__class__.__name__)
            td(ds.description)
            res.append('</tr>')
        res.append('</table>')
        return "".join(res)


    @classmethod
    def from_yaml(cls, *paths):
        if isinstance(paths[0],(list,tuple)):
            paths = paths[0]

        config = merge_dicts(*[read_yaml(path) for path in paths])
        table_metas = config.get('table_metadata',{})
        data_sources = config.get('data_source',{})

        reg_dict = {}
        for ds_name, info in data_sources.items():
            module_name,class_name = info['class'].rsplit('.',1)
            _log.debug('%s %s',module_name, class_name)
            module = importlib.import_module(module_name)
            _class = getattr(module, class_name)
            _log.debug('%s', _class)

            reg_dict[ds_name] = _class.from_config(ds_name, config)
            
        return cls(reg_dict)
        
