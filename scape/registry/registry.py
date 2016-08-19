from __future__ import absolute_import
from .parsing import parse_list_fieldselectors
from collections import defaultdict as _ddict

class Registry(dict):
    '''A collection of data sources.

    Args:

      data_sources (Dict[:class:`DataSource`])): dictionary from data
        source names to DataSource objects

    Example:

        >>> registry = Registry( {
        ... })
        >>>

    '''

    def __init__(self, data_sources):
        self.update(data_sources)
        for k,ds in self.items():
            if ds.name == 'Unknown':
                ds._name = k

    def has(self, field_selectors):
        '''
        Get the set of data sources containing a field selector.
        '''
        selectors = parse_list_fieldselectors(field_selectors)
        res = {}
        for k,ds in self.items():
            for selector in selectors:
                if ds._metadata.fields_matching(selector):
                    res[k]=ds
        return _Selection(res, selectors)

    def all_fields(self):
        return _FieldSelection(self)

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
            td(ds.description if ds.description else '')
            res.append('</tr>')
        res.append('</table>')
        return "".join(res)


class _Selection(Registry):
    '''The results of a registry.has() call with a specialized html output format'''
    def __init__(self, data_sources, field_selectors):
        super(_Selection, self).__init__(data_sources)
        self.field_selectors = field_selectors

    def _repr_html_(self):
        res = ['<table>']
        def td(d):
            res.extend(['<td>',d,'</td>'])
        def data_source_html(tm):
            fields = set([f for selector in self.field_selectors for f in tm.fields_matching(selector)])
            for f in fields:
                res.extend('<tr>')
                td('')
                td('')
                td('')
                td(f.name)
                taggeddim = tm.field_tagged_dim(f)
                if taggeddim.dim:
                    td(taggeddim.dim.name)
                else:
                    td('')
                td(','.join((t.name for t in taggeddim.tags)))
                res.extend('</tr>\n')

        res.append('<th>Name</th><th>Class</th><th>Description</th><th>Field</th><th>Dim</th><th>Tags</th></tr>\n')
        for k in sorted(self.keys()):
            ds = self[k]
            res.append('<tr>')
            td(k)
            td(ds.__class__.__name__)
            td(ds.description)
            td('')
            td('')
            td('')
            res.append('</tr>\n')

            data_source_html(ds.metadata)
        res.append('</table>\n')
        return "".join(res)

class _FieldSelection(Registry):
    def _repr_html_(self):
        res = ['<table>']
        def th(*ts):
            for t in ts:
                res.extend(['<th>', t, '</th>'])
        def c(*ts):
            for t in ts:
                res.extend(['<td>', t, '</t>'])
        th('Field','Dim', 'Tags', 'Data Sources')
        fields = _ddict(lambda: _ddict(lambda: set()))
        for ds in self.values():
            for f,td in ds.metadata._map.items():
                td_to_ds = fields[f]
                td_to_ds[(td.dim.name if td.dim else None, td.tags)].add(ds.name)
        def tags(ts):
            return ", ".join([t.name for t in ts])
        for f in sorted(fields.keys()):
            td_to_ds = fields[f]
            for taggeddim,ds in td_to_ds.items():
                res.extend('<tr>')
                c(f, taggeddim[0], tags(taggeddim[1]), ", ".join(list(ds)))
                res.extend('</tr>')
        res.append('</table>')
        return "".join(res)
