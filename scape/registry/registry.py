from __future__ import absolute_import

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


