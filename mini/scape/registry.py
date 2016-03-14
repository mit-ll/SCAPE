class TagsDim(object):
    def __init__(self, tags=[], dim=None):
        for t in tags:
            if not isinstance(t,Tag):
                raise ValueError("Expecting a Tag, not " + str(t) + ' ' + str(type(t)))
        if dim and not isinstance(dim,Dim):
                raise ValueError("Expecting a Dim, not " + str(dim) + str(type(t)))
        self._tags = frozenset(tags)
        self._dim = dim
    def __repr__(self):
        dstr = self._dim.__repr__() if self._dim else "None"
        return "TagsDim(" + self._tags.__repr__() + ", " + dstr + ")"
    @property
    def tags(self):
        return self._tags
    @property
    def dim(self):
        return self._dim

class Field(object):
    def __init__(self, name):
        self._name = name
    def __repr__(self):
        return "Field(" + self._name + ")"        
    @property
    def name(self):
        return self._name

class Dim(object):
    def __init__(self,d):
        self._dim = d
    def __repr__(self):
        return "Dim(" + self._dim + ")"
    def __eq__(self, other):
        return isinstance(other, Dim) and self._dim == other._dim
    def __hash__(self):
        return hash(self._dim)

class Tag(object):
    def __init__(self, t):
        self._tag = t
    def __repr__(self):
        return "Tag('" + self._tag + "')"
    def __eq__(self, other):
        return isinstance(other, Tag) and self._tag == other._tag
    def __hash__(self):
        return hash(self._tag)

class Registry(object):
    def __init__(self, data_sources):
        self._data_sources = data_sources

    def data_source(self,name):
        return self._data_sources[name]
    

class TableMetadata(object):
    def _from_map(self,m):
        for (k,v) in m.items():
            tags = [Tag(t) for t in (v['tags'] if 'tags' in v else [])]
            dim = Dim(v['dim']) if 'dim' in v else None
            yield (k,TagsDim(tags,dim))
            
    def __init__(self,m):
        self._map = dict(self._from_map(m))

    def _tagsdim_subset(self,x,y):
        dims_match = not x.dim or (y.dim and set([x.dim]).issubset(set([y.dim])))
        return (x.tags.issubset(y.tags)) and (dims_match)
        
    def tagsdim_matches(self,td,field):
        if field.name not in self._map:
            return False
        metadata_td = self._map[field.name]
        return self._tagsdim_subset(td, metadata_td)
        
    def fields_matching(self,td):
        return [f for f,ftd in self._map.items() if self.tagsdim_matches(td,Field(f))]
    
    def __repr__(self):
        return str(self._map)

class DataSource(object):
    pass
