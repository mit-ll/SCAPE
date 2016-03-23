from scape.registry import Tag, Dim, TagsDim, TableMetadata

def tag(t):
    return t if isinstance(t,Tag) else Tag(t)

def dim(d):
    if not d:
        return None
    if isinstance(d,Dim):
        return d
    if isinstance(d,str):
        return Dim(d)
    else:
        raise ValueError("Expecting Dim, str, or None. Not " + str(d))

def _dim(d):
 """ An alias to dim(), allowing functions to call dim() while have a parameters or variables named dim."""
 return dim(d)

def td(dim=None, *tags):
    tags = [tag(t) for t in tags] if tags else []
    return TagsDim(tags, _dim(dim))

def tagsdim(td):
    if type(td) in (list,tuple):
        elements = td
    elif isinstance(td,str):
        strd = td
        if ':' in strd:
            elements = strd.split(':')
        else:
            elements = [strd]
    else:
        raise ValueError("Expecting string or list of strings, not " + str(td))
    d = _dim(elements[-1].strip())
    tags = []
    for rawtag in elements[:-1]:
        t = rawtag.strip()
        if t:
            tag = Tag(t)
            tags.append(tag)
    return TagsDim(tags=tags, dim=d)

def _create_table_field_tagsdim_map(m):
    if isinstance(m, TableMetadata):
        return m
    elif isinstance(m, dict):
        return TableMetadata(m)
    else:
        raise ValueError("Expecting a dictionary, or TableMetadata, not " + str(type(m)))
