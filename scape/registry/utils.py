from six import string_types

from .field import Field
from .tagged_dim import TaggedDim, tagged_dim

def field_or_tagged_dim(x):
    ''' Type routing function for Field or TagDims information
    '''
    if isinstance(x, string_types):
        if x.startswith("@"):
            return Field(x[1:])
        else:
            return tagged_dim(x)
    elif isinstance(x, Field):
        return x
    elif isinstance(x, TaggedDim):
        return x
    elif type(x) in (list, tuple):
        return tagged_dim(x)
    else:
        raise ValueError(
            "Expecting Field, TaggedDim, string or list of strings,"
            " not {} of type {}".format(td,type(td))
        )
field_or_tagged_dim = field_or_tagged_dim

