'''Registry conditional parsing

'''
from __future__ import absolute_import
from __future__ import print_function

import pyparsing
from pyparsing import srange, nums, quotedString, delimitedList
from pyparsing import Combine, Word, LineStart, LineEnd, Optional, Literal

from .condition import Condition, GenericBinaryCondition, GenericSetCondition
from .utils import field_or_tagged_dim
import sys,traceback

def rhs_value_p():
    Ipv4Address = Combine(Word(nums) + ('.'+Word(nums))*3).setResultsName('ipv4')
    Ipv4Address = Ipv4Address.setParseAction(lambda s, l, toks: toks[0])

    Int = Word(nums)
    Int = Int.setParseAction(lambda s, l, toks: int(toks[0]))

    Float = Combine(Word(nums) + '.' + Word(nums)).setResultsName('float')
    Float = Float.setParseAction(lambda s, l, toks: float(toks[0]))

    String = quotedString.copy().addParseAction(pyparsing.removeQuotes)

    rhs = pyparsing.Or([String, Int, Float, Ipv4Address])
    return rhs

def rhs_set_p():
    lb = pyparsing.Literal('{').suppress()
    rb = pyparsing.Literal('}').suppress()
    rhs_value = rhs_value_p()
    wordList = pyparsing.delimitedList(rhs_value)
    pat1 = (lb + wordList + rb) #.addParseAction(lambda s,l,toks: toks[:])
    return pat1

def rhs_p():
    val = rhs_value_p().setResultsName('value')
    valset = rhs_set_p().setResultsName('valueset')
    rhs = pyparsing.Or([val, valset])
    return rhs

def tagdim_field_p():
    td = Word(srange('[-_a-zA-Z0-9:]')).setResultsName('tagsdim')
    f = Combine('@' + Word(srange('[_.a-z0-9A-Z]+'))).setResultsName('field')
    parser = (f | td).setParseAction(
        lambda s, l, toks: field_or_tagged_dim(toks[0])
    )
    return parser

def list_tagdim_field_p():
    def p(s, l, toks):
        return toks[0]
    star = Literal('*').setResultsName('star').setParseAction(p)
    optTds = Optional(delimitedList(tagdim_field_p())).setParseAction(lambda s, l, toks: toks)
    return star |  optTds

def binary_condition_p():
    lhs = tagdim_field_p().setResultsName('lhs')
    op = Word('[!=<>~]').setResultsName('op')

    rhs = rhs_p().setResultsName('rhs')
    line = (LineStart() + lhs + op + rhs + LineEnd())

    def cond(s,l,toks):
        if 'value' in toks:
            return GenericBinaryCondition(toks['lhs'], toks['op'], toks['value'])
        elif 'valueset' in toks:
            return GenericSetCondition(toks['lhs'], toks['op'], toks['valueset'][:])
        else:
            raise ValueError("Could not parse {}".format(toks))

    line.setParseAction(cond)
    return line

def parse_list_fieldselectors(fields):
    if isinstance(fields, (list,tuple)):
        return fields
        # return sum([parse_list_fieldselectors(f) for f in fields], [])
    r = list_tagdim_field_p().parseString(fields, parseAll=True).asList()
    if len(r) >= 1 and r[0] == '*':
        r = []
    return r

def parse_binary_condition(condition):
    if isinstance(condition, Condition):
        return condition
    elif not isinstance(condition, str):
        raise ValueError(
            "Expecting string or Condition, not {}={}".format(
                type(condition),condition
            )
        )
    return binary_condition_p().parseString(condition)[0]


