from scape.registry import Tag, Dim, TagsDim, TableMetadata

def tagsdim(d):
    if type(d) in (list,tuple):
        elements = d
    else:
        strd = str(d)
        if ':' in strd:
            elements = strd.split(':')
        else:
            elements = [strd]
    d = elements[-1].strip()
    dim = Dim(d) if d else None
    tags = []
    for rawtag in elements[:-1]:
        t = rawtag.strip()
        if t:
            tag = Tag(t)
            tags.append(tag)
    return TagsDim(tags=tags, dim=dim)

def _create_table_field_tagsdim_map(m):
    if isinstance(m, TableMetadata):
        return m
    elif isinstance(m, dict):
        return TableMetadata(m)
    else:
        raise ValueError("Expecting a dictionary, or table metadata")
