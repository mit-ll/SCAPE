from __future__ import absolute_import
from .parsing import parse_list_fieldselectors

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

    def has(self, field_selectors):
        '''
        Get the set of data sources containing a field selector.
        '''
        selectors = parse_list_fieldselectors(field_selectors)
        res = {}
        for k,v in self.items():
            if any((v._metadata.fields_matching(selector) for selector in selectors)):
                res[k]=v
        return _Selection(res, selectors)

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
