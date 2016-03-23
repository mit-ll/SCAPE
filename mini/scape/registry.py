class TagsDim(object):
    """ A field selector containing any number of tags and an optional dimension.
    """
    def __init__(self, tags=[], dim=None):
        """ Create a TagsDim"""
        for t in tags:
            if not isinstance(t,Tag):
                raise ValueError("Expecting a Tag, not " + str(t) + ' ' + str(type(t)))
        if dim and not isinstance(dim,Dim):
                raise ValueError("Expecting a Dim, not " + str(dim) + ' ' + str(type(dim)))
        self._tags = frozenset(tags)
        self._dim = dim

    def __repr__(self):
        dstr = self._dim.__repr__() if self._dim else "None"
        return "TagsDim(" + self._tags.__repr__() + ", " + dstr + ")"

    @property
    def tags(self):
        """ The collection of tags"""
        return self._tags

    @property
    def dim(self):
        """ The dimension or None"""
        return self._dim

class Field(object):
    """ The name of a field in a data source. """
    def __init__(self, name):
        self._name = name

    def __repr__(self):
        return "Field(" + self._name + ")"

    @property
    def name(self):
        """ The name of the field """
        return self._name

class Dim(object):
    """ """
    def __init__(self,d):
        if not isinstance(d,str):
            raise ValueError("Expecting a string, not " + str(d))
        self._dim = d

    def __repr__(self):
        return "Dim(" + self._dim + ")"

    def __eq__(self, other):
        return isinstance(other, Dim) and self._dim == other._dim

    def __hash__(self):
        return hash(self._dim)

class Tag(object):
    """  """
    def __init__(self, t):
        if not isinstance(t,str):
                raise ValueError("Expecting a string, not " + str(t))
        self._tag = t

    def __repr__(self):
        return "Tag('" + self._tag + "')"

    def __eq__(self, other):
        return isinstance(other, Tag) and self._tag == other._tag

    def __hash__(self):
        return hash(self._tag)

class Registry(object):
    """ A collection  of data sources. """

    def __init__(self, data_sources):
        """A dictionary from data source names to data source."""
        self._data_sources = data_sources

    def data_source(self,name):
        return self._data_sources[name]
    

class TableMetadata(object):
    """ TableMetadata provides logic to map tag/dimension selectors to sets of fields. """
    def _from_map(self,m):
        for (k,v) in m.items():
            if isinstance(v, TagsDim):
                yield (k,v)
            else:
                tags = [Tag(t) for t in (v['tags'] if 'tags' in v else [])]
                dim = Dim(v['dim']) if 'dim' in v and v['dim'] else None
                yield (k,TagsDim(tags,dim))
            
    def __init__(self,m):
        """ Construct table metadata from a dictionary of tags and dimensions

        Example:
            TableMetadata({
               'field1': { 'tags' : [ 'tag1', 'tag2' ], 'dim' : 'dim1' },
               'field2': { 'tags' : [ 'tag1', 'tag2' ] },
               'field3': { 'dim' : 'dim1' },
               'field4': { } })
        """
        self._map = dict(self._from_map(m))

    def _tagsdim_subset(self,x,y):
        """ Return if the TagsDim `x` is compatible with `y`  
        
        x is compatible if it has no dimension or the same dimension as y and if y contains a subset of the tags of x.
        """
        dims_match = not x.dim or (y.dim and set([x.dim]).issubset(set([y.dim])))
        return (x.tags.issubset(y.tags)) and (dims_match)
        
    def tagsdim_matches(self,tagsdim,field):
        """ Return true if `tagsdim` matches `field`.
        
        TODO: Should this raise an exception on fields not included in the table metadata?
        """
        if field.name not in self._map:
            return False
        metadata_td = self._map[field.name]
        return self._tagsdim_subset(tagsdim, metadata_td)
        
    def fields_matching(self,tagsdim):
        """Get the list of all fields matching `tagsdim`.
        """
        return [f for f,ftd in self._map.items() if self.tagsdim_matches(tagsdim,Field(f))]
    
    @property
    def fields(self):
        """ Get an iterator of field """
        return ((Field(f) for f in self._map.keys()))

    def __repr__(self):
        return str(self._map)

class DataSource(object):
    pass
