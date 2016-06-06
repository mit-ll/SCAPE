import re
from nose.tools import *

from scape.registry import *
import scape.registry

import pyparsing

# Dimension #############################################################

@raises(ValueError)
def test_dim_badarg():
    dim(1)

@raises(ValueError)
def test_dim_badarg2():
    Dim(1)

def test_dim_repr():
    d = dim('ip')
    assert_equal( d , eval(repr(d)))

def test_dim_html():
    d = dim('ip')
    d._repr_html_()

def test_parse_dims():
    assert_equal( dim(None) , None)
    assert_equal( dim('src') , Dim('src'))
    d = dim('src')
    assert_equal( dim(d) , d)

# Tag ##################################################################

def test_tag_repr():
    r = Tag('tag')
    assert_equal( r , eval(repr(r)))

def test_tag_html():
    Tag('g')._repr_html_()

@raises(ValueError)
def test_bad_arg():
    Tag(1)

def test_tag():
    assert_equal( tag('t') , Tag('t'))

# Field ################################################################

def test_field_repr():
    r = Field('f')
    assert_equal( r , eval(repr(r)))

# TagsDim ##############################################################

@raises(ValueError)
def test_tagsdim_str():
    TagsDim(tags=["asdf"])

@raises(ValueError)
def test_tagsdim_baddim():
    TagsDim(dim=1)

@raises(ValueError)
def test_tagsdim_badarg():
    tagsdim(1)

def test_tagsdim_repr():
    t = tagsdim("tag1:dim")
    assert_equal( t , eval(repr(t)))

def test_parse_tagsdim():
    assert_equal( tagsdim(['tag1', 'dim']) , TagsDim(dim=Dim('dim'), tags=[Tag('tag1')]))
    assert_equal( tagsdim('dim') , TagsDim(dim=Dim('dim')))
    assert_equal( tagsdim(':dim') , TagsDim(dim=Dim('dim')))
    assert_equal( tagsdim('tag1:') , TagsDim(tags=[Tag('tag1')]))
    assert_equal( tagsdim('tag1:dim') , TagsDim(dim=Dim('dim'), tags=[Tag('tag1')]))
    assert_equal( tagsdim('tag1:tag2:dim') , TagsDim(dim=Dim('dim'), tags=[Tag('tag1'), Tag('tag2')]))

# Binary Condition #####################################################

def test_parse_field_eq_num():
    assert_equal( scape.registry._parse_binary_condition('@asdf == 23') , GenericBinaryCondition(Field('asdf'), '==', 23))
def test_parse_field_eq_float():
    scape.registry._parse_binary_condition('@asdf == 2.03')
def test_parse_field_eq_dblquote():
    scape.registry._parse_binary_condition('@asdf == "asdf"')
def test_parse_field_eq_quote():
    scape.registry._parse_binary_condition("@asdf == 'asdf'")
def test_parse_field_eq_ip():
    scape.registry._parse_binary_condition('@asdf == 2.3.4.5')

# We don't yet support ()'s, ands, ors
#def test_parse_field_eq_parens():
#    scape.registry._parse_binary_condition('(@asdf == 2.3.4.5)')

@raises(pyparsing.ParseException)
def test_parse_field_eq_ip_extra_garbage():
    scape.registry._parse_binary_condition('@asdf == 2.3.4.5  ffff')

@raises(pyparsing.ParseException)
def test_parse_field_eq_unquoted():
    scape.registry._parse_binary_condition("@asdf == asdf")

def test_parse_dim_eq_num():
    scape.registry._parse_binary_condition(':dim == 23')
def test_parse_tag_eq_num():
    scape.registry._parse_binary_condition('tag: == 23')
def test_parse_tagdim_eq_num():
    scape.registry._parse_binary_condition('tag:dim == 23')
def test_parse_tags_eq_num():
    scape.registry._parse_binary_condition("tag1:tag2 == 23")


# Field Selectors ######################################################

def test_parse_tagdim_field_list_fields1():
    def f(x):
        res = scape.registry._parse_list_fieldselectors(x)
        return res
    assert_equal( f("*"),[])
    assert_equal( f(""),[])
    assert_equal( f("@F"),[Field('F')])
    assert_equal( f("@F,@G"),[Field('F'),Field('G')])
    assert_equal( f(":dim"),[tagsdim("dim")])
    assert_equal( f("tag:,:dim"),[tagsdim('tag:'),tagsdim("dim")])

@raises(pyparsing.ParseException)
def test_bad_fieldselectors():
    scape.registry._parse_list_fieldselectors("@categories == 'General'")


# Table Metadata #######################################################

def test_tablemeta_of_dicts():
    TableMetadata({'clientip' : { 'tags' : ['client'], 'dim':'ip' }})

tm = TableMetadata({'clientip' : { 'tags' : ['client'], 'dim':'ip' }})

def test_tablemeta_html():
    tm._repr_html_()

def test_tm_tdselect():
    assert_true( tm._tagsdim_subset(tagsdim(':'), tagsdim('src:ip')) )
    assert_true( tm._tagsdim_subset(tagsdim(':ip'), tagsdim('src:ip')) )
    assert_true( not tm._tagsdim_subset(tagsdim(':mac'), tagsdim('src:ip')) )

    assert_true( tm._tagsdim_subset(tagsdim('src:'), tagsdim('src:ip')) )
    assert_true( not tm._tagsdim_subset(tagsdim('src:mac'), tagsdim('src:ip')) )
    assert_true( not tm._tagsdim_subset(tagsdim('src:nat:ip'), tagsdim('src:ip')) )

    assert_true( tm._tagsdim_subset(tagsdim('src:'), tagsdim('src:')) )
    assert_true( tm._tagsdim_subset(tagsdim('src:'), tagsdim('src:nat:')) )

def test_tagsdim_matchs():
    # not in table metadata
    assert_true( not tm.tagsdim_matches(tagsdim(':'), Field('ip_addr')) )

    assert_true( tm.tagsdim_matches(tagsdim(':'), Field('clientip')) )
    assert_true( tm.tagsdim_matches(tagsdim(':ip'), Field('clientip')) )
    assert_true( not tm.tagsdim_matches(tagsdim(':mac'), Field('clientip')) )

def test_fields_matching():
    assert_equal( tm.fields_matching(Field('clientip')) , [Field('clientip')])
    assert_equal( tm.fields_matching(tagsdim(':ip')) , [Field('clientip')])
    assert_equal( tm.fields_matching(tagsdim(':mac')) , [])

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

# Conditions ###########################################################

def test_condition():
    c = Condition()
    assert_equal( c.fields , [])
    assert_equal( c.map(lambda x:x) , c)

### Binary Condition
def test_bc_repr():
    c = BinaryCondition(tagsdim('t:d'), 2)
    assert_equal( c , eval(repr(c)))

### Generic Binary Condition

def test_gbc_repr():
    c = GenericBinaryCondition(tagsdim('t:d'), '==', 2)
    assert_equal( c , eval(repr(c)))

### Equals

def test_equals_repr():
    c = Equals(tagsdim('t:d'), 2)
    assert_equal( c , eval(repr(c)))

### Matches

def test_matches_repr():
    c = MatchesCond(tagsdim('t:d'), 2)
    assert_equal( c , eval(repr(c)))

### Greater Than

def test_gt_repr():
    c = GreaterThan(tagsdim('t:d'), 2)
    assert_equal( c , eval(repr(c)))

### Greater Than Equal To

def test_gteq_repr():
    c = GreaterThanEqualTo(tagsdim('t:d'), 2)
    assert_equal( c , eval(repr(c)))

### Or

def test_or_repr():
    c = Or([Equals(tagsdim('src:ip'), 2)])
    assert_equal( c , eval(repr(c)))

def test_or_fields():
    c = Or([Equals(Field('x'), 2)])
    assert_equal( [Field('x')] , list(c.fields))

def flip(x):
    if isinstance(x, Equals):
        return Equals(x.lhs, 2 * x.rhs)
    elif isinstance(x, Or):
        return And(x.parts)
    elif isinstance(x, And):
        return Or(x.parts)
    else:
        return x

def test_or_map():
    c = Or([Equals(Field('x'), 2)])
    actual = c.map(flip)
    expected = And([Equals(Field('x'), 4)])
    assert_equal( actual , expected)

def test_or_leaves_map():
    c = Or([Equals(Field('x'), 2)])
    actual = c.map_leaves(flip)
    expected = Or([Equals(Field('x'), 4)])
    assert_equal( actual , expected)

@raises(ValueError)
def test_or_condition0():
    scape.registry._or_condition([])

def test_or_condition1():
    e = Equals(Field('x'), 2)
    assert_equal( e , scape.registry._or_condition([e]))

def test_or_condition_many():
    cs = [Equals(Field('x'), 2), Equals('y', 3)]
    assert_equal( cs , scape.registry._or_condition([cs]))

### And

def test_and_repr():
    a = And([Equals(Field('x'), 2)])
    assert_equal( a , eval(repr(a)))

def test_and_fields():
    a = And([Equals(Field('x'), 2)])
    assert_equal( [Field('x')] , list(a.fields))

def test_and_map():
    x = And([Equals(Field('x'), 2)])
    actual = x.map(flip)
    expected = Or([Equals(Field('x'), 4)])
    assert_equal( actual , expected)

def test_and_map_leaves():
    a = And([Equals(Field('x'), 2)])
    assert_equal( And([Equals(Field('x'), 4)]) , a.map_leaves(flip))


weblog_metadata = TableMetadata({
    'clientip' : tagsdim('client:ip'),
    'serverip' : tagsdim('server:ip'),
    'url' : tagsdim('http:url'),
    'status_code' : tagsdim('http:status_code'),
    'time': tagsdim('sec')
})

weblog_data = [ {
    'clientip' : '1.2.3.4',
    'serverip' : '4.4.4.4',
    'url' : 'http://foo.bar.com/index.html',
    'status_code' : '404',
    'time' : '03-31-2011 08:11:22'
},  {
    'clientip' : '7.8.9.2',
    'serverip' : '4.4.4.4',
    'url' : 'http://foo.bar.com/status.html',
    'status_code' : '200',
    'time' : '03-31-2011 08:14:33'
}, {
    'clientip' : '1.2.3.4',
    'serverip' : '4.4.4.4',
    'url' : 'http://quux.com/index.html',
    'status_code' : '200',
    'time' : '03-31-2011 09:23:11'
}, {
    'clientip' : '1.2.3.4',
    'serverip' : '4.4.4.5',
    'url' : 'http://biz.com/index.html',
    'status_code' : '200',
    'time' : '04-01-2011 01:10:11'
}]


def interpret(cond):
    if isinstance(cond, Equals):
        return lambda r: r[cond.lhs.name]==cond.rhs
    elif isinstance(cond, GreaterThan):
        return lambda r: r[cond.lhs.name]>cond.rhs
    elif isinstance(cond, GreaterThanEqualTo):
        return lambda r: r[cond.lhs.name]>=cond.rhs
    elif isinstance(cond, MatchesCond):
        return lambda r: re.match(cond.rhs, r[cond.lhs.name])
    elif isinstance(cond, And):
        return lambda r: all([interpret(c)(r) for c in cond.parts])
    elif isinstance(cond, Or):
        return lambda r: any([interpret(c)(r) for c in cond.parts])
    elif isinstance(cond, TrueCondition):
        return lambda r: True
    else:
        raise ValueError("Unexpected condition {}".format(str(cond)))

class PythonDataSource(DataSource):
    def __init__(self, metadata, data):
        super(PythonDataSource, self).__init__(metadata, "description", {
            '==': Equals,
            '=~':  MatchesCond
        })
        self._data = data

    def run(self, select):
        self.check_select(select)

        field_names = self.field_names(select)
        def proj(x):
            return {k:v for k, v in x.items() if k in field_names}

        cond = self._rewrite(select._condition)
        f = interpret(cond)
        return [proj(f) for f in filter(f, self._data)]

ds =  PythonDataSource(weblog_metadata, weblog_data)

# Data Source ##########################################################

def test_ds_default_name():
    assert_equal( ds.name , "Unknown")

@raises(NotImplementedError)
def test_not_implmented_run():
    DataSource(TableMetadata({}),description="", op_dict={}).select().run()

def test_ds_properties():
    ds.name
    ds.metadata

def test_ds_repr():
    repr(ds)

def test_ds_html():
    ds._repr_html_()

# rewrite_generic_binary_condition

def test_ds_remove_generic_binary_condition():
    c = GenericBinaryCondition(Field('f'),'==', '23')
    r = ds._rewrite_generic_binary_condition(c)
    assert_equal( r , Equals(Field('f'), '23'))

def test_ds_unnecessary_remove_generic_binary_condition():
    c = Equals(Field('f'),'23')
    r = ds._rewrite_generic_binary_condition(c)
    assert_equal( c , r)

@raises(ValueError)
def test_ds_remove_generic_binary_condition_unsupported_op():
    c = GenericBinaryCondition(Field('f'),'<>', '23')
    ds._rewrite_generic_binary_condition(c)

# rewrite_tagsdim
def gbceq(l,r):
    return GenericBinaryCondition(l, '==', r)

def test_ds_rewrite_tagsdim_to_or():
    c = gbceq(tagsdim(':ip'), '1.2.3.4')
    r = ds._rewrite_tagsdim(c)
    expected = Or([gbceq(Field('clientip'), '1.2.3.4'), gbceq(Field('serverip'), '1.2.3.4')])
    assert_equal( expected , r)

def test_ds_rewrite_tagsdim_nogbc():
    c = Equals(tagsdim(':ip'), '1.2.3.4')
    r = ds._rewrite_tagsdim(c)
    assert_equal( c , r)

@raises(ValueError)
def test_ds_rewrite_tagsdim_no_fields():
    c = gbceq(tagsdim(':mac'), '1:2:3:4:5:6')
    # No fields have dimension=mac
    ds._rewrite_tagsdim(c)

# rewrite_outer_and

def test_and0():
    actual = ds._rewrite_outer_and(And([]))
    assert_equal( TrueCondition() , actual)

def test_and1():
    a = Equals(Field('a'), '1')
    actual = ds._rewrite_outer_and(And([a]))
    assert_equal( a , actual)

def test_and_rparen():
    a = Equals(Field('a'), '1')
    b = Equals(Field('b'), '1')
    c = Equals(Field('d'), '1')
    actual = ds._rewrite_outer_and(And([And([a,b]),c]))
    assert_equal( And([a,b,c]) , actual)

def test_select_all():
    assert_equal( len(ds.select().run()) , len(weblog_data))

# check fields

@raises(ValueError)
def test_check_fields():
    ds._check_fields(Equals(Field('a'), '1'))


# Registry #############################################################

def test_registry():
    r = Registry({'testds':ds})

def test_html():
    r = Registry({'testds':ds})
    r._repr_html_()


# Select ###############################################################

def test_select_repr():
    # TODO DataSource not round tripping
    repr(ds.select(':ip'))

def test_select_copy():
    q = ds.select(':ip')
    q2 = q.copy()
    # TODO __eq__ for select, need to account for attrs
#    assert q == q2

def test_add_where():
    q = ds.select(':ip')
    q2 = q.where(':ip==1.2.3.4')
    q3 = q.where(Equals(Field('clientip'), '1.2.3.4'))

@raises(ValueError)
def test_add_where_badarg():
    q = ds.select(':ip')
    q2 = q.where(1)

def test_select_set_fields():
    q = ds.select(':ip')
    q2 = q.with_fields(':mac')

def test_select_check():
    ds.select(':ip').check()

def test_select_debug():
    ds.select(':ip').debug()

def test_select_fields():
    pass
#    print(ds.select(':ip').run())
#    assert [Field('clientip'), Field('serverip')] == ds.select(':ip')._fields
