import re

from nose.tools import *

from scape.registry.field import Field
from scape.registry.tag import Tag, tag
from scape.registry.dim import Dim, dim
from scape.registry.tagged_dim import TaggedDim, tagged_dim
from scape.registry.table_metadata import TableMetadata

# Table Metadata #######################################################

def test_tablemeta_of_dicts():
    TableMetadata({'clientip' : { 'tags' : ['client'], 'dim':'ip' }})

tm = TableMetadata({'clientip' : { 'tags' : ['client'], 'dim':'ip' }})

def test_tablemeta_html():
    tm._repr_html_()

def test_tm_tdselect():
    assert_true( tm._tagged_dim_subset(tagged_dim(':'), tagged_dim('src:ip')) )
    assert_true( tm._tagged_dim_subset(tagged_dim(':ip'), tagged_dim('src:ip')) )
    assert_true( not tm._tagged_dim_subset(tagged_dim(':mac'), tagged_dim('src:ip')) )

    assert_true( tm._tagged_dim_subset(tagged_dim('src:'), tagged_dim('src:ip')) )
    assert_true( not tm._tagged_dim_subset(tagged_dim('src:mac'), tagged_dim('src:ip')) )
    assert_true( not tm._tagged_dim_subset(tagged_dim('src:nat:ip'), tagged_dim('src:ip')) )

    assert_true( tm._tagged_dim_subset(tagged_dim('src:'), tagged_dim('src:')) )
    assert_true( tm._tagged_dim_subset(tagged_dim('src:'), tagged_dim('src:nat:')) )

def test_tagged_dim_matchs():
    # not in table metadata
    assert_true( not tm.tagged_dim_matches(tagged_dim(':'), Field('ip_addr')) )

    assert_true( tm.tagged_dim_matches(tagged_dim(':'), Field('clientip')) )
    assert_true( tm.tagged_dim_matches(tagged_dim(':ip'), Field('clientip')) )
    assert_true( not tm.tagged_dim_matches(tagged_dim(':mac'), Field('clientip')) )

def test_fields_matching():
    assert_equal( tm.fields_matching(Field('clientip')) , [Field('clientip')])
    assert_equal( tm.fields_matching('@clientip') , [Field('clientip')])
    assert_equal( tm.fields_matching(tagged_dim(':ip')) , [Field('clientip')])
    assert_equal( tm.fields_matching(tagged_dim(':mac')) , [])

@raises(ValueError)
def test_fields_matching_badarg():
    assert_true( tm.fields_matching(1) )

def test_has_field():
    assert_true( tm.has_field(Field('clientip')) )
    assert_true( not tm.has_field(Field('asdf')) )

def test_properties():
    tm.fields
    tm.field_names

def test_repr():
    repr(tm) # no equality defined for table metadata

def test_tags():
    assert_equal(set(['client']), tm.tags)

def test_dims():
    assert_equal(set(['ip']), tm.dims)
