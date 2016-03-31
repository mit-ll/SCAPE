import re

from scape.registry import *
import scape.registry as r


def test_parse_dims():
    assert dim(None) == None
    assert dim('src') == Dim('src')
    d = dim('src')
    assert dim(d) == d

def test_parse_tagsdim():
    assert tagsdim('dim') == TagsDim(dim=Dim('dim'))
    assert tagsdim(':dim') == TagsDim(dim=Dim('dim'))
    assert tagsdim('tag1:') == TagsDim(tags=[Tag('tag1')])
    assert tagsdim('tag1:dim') == TagsDim(dim=Dim('dim'), tags=[Tag('tag1')])
    assert tagsdim('tag1:tag2:dim') == TagsDim(dim=Dim('dim'), tags=[Tag('tag1'), Tag('tag2')])

def test_parse_field_eq_num():
    assert r._parse_binary_condition('@asdf == 23') == GenericBinaryCondition(Field('asdf'), '==', 23)
def test_parse_field_eq_float():
    r._parse_binary_condition('@asdf == 2.03')
def test_parse_field_eq_dblquote():
    r._parse_binary_condition('@asdf == "asdf"')
def test_parse_field_eq_quote():
    r._parse_binary_condition("@asdf == 'asdf'")
def test_parse_field_eq_ip():
    r._parse_binary_condition('@asdf == 2.3.4.5')

def test_parse_dim_eq_num():
    r._parse_binary_condition(':dim == 23')
def test_parse_tag_eq_num():
    r._parse_binary_condition('tag: == 23')
def test_parse_tagdim_eq_num():
    r._parse_binary_condition('tag:dim == 23')
def test_parse_tags_eq_num():
    r._parse_binary_condition("tag1:tag2 == 23")


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
    elif isinstance(cond, MatchesCond):
        return lambda r: re.match(cond.rhs, r[cond.lhs.name])
    elif isinstance(cond, And):
        return lambda r: all([interpret(c)(r) for c in cond.xs])
    elif isinstance(cond, Or):
        return lambda r: any([interpret(c)(r) for c in cond.xs])
    else:
        raise ValueError("Unexpected condition {}".format(str(cond)))


class PythonDataSource(DataSource):
    def __init__(self, metadata, data):
        super(PythonDataSource, self).__init__(metadata, {
            '==': Equals,
            '=~':  MatchesCond
        })
        self._data = data

    def run(self, select):
        self.check_query(select._condition)
#        print(self._fields_or_tagsdim)
#        print(self._condition)
        cond = self._rewrite(select._condition)
        f = interpret(cond)
        return list(filter(f, self._data))
    
ds =  PythonDataSource(weblog_metadata, weblog_data)
#print('test')
#print(ds.metadata)

def test_select_all():
    assert len(ds.select().run()) == len(weblog_data)

#def test_select_eq():
#    assert len(ds.select().where("@clientip == 7.8.9.2").run()) == 1

