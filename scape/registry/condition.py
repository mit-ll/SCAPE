from __future__ import absolute_import

from .field import Field

class Condition(object):
    '''Base class for conditions in search phrases

    Example:

      >>> select = R.select('*').where('source:ip == "192.168.1.1"')
    
      ``'source:ip == "192.168.1.1"'`` is the search phrase,
      corresponding to an :class:`Equals` condition

    '''
    def copy(self):
        raise NotImplementedError('need to implement in subclass')
    
    @property
    def fields(self):
        return []

    def map(self, f):
        return f(self)

    def map_leaves(self, f):
        return f(self)

class TrueCondition(Condition):
    def __init__(self):
        pass

    def __eq__(self, other):
        return type(self) == type(other)

class ConstituentCondition(Condition):
    def __init__(self, parts):
        self._parts = parts

    def copy(self):
        copy = type(self)([p.copy() for p in self._parts])
        return copy

    def __repr__(self):
        return "{}({!r})".format(type(self).__name__, self._parts)

    def __eq__(self, other):
        return type(self) == type(other) and frozenset(self.parts) == frozenset(other.parts)

    @property
    def parts(self):
        return self._parts[:]

    @property
    def fields(self):
        fields = []
        for part in self._parts:
            fields.extend(part.fields)
        return fields

    def map(self, func):
        return func(type(self)([func(x) for x in self._parts]))

    def map_leaves(self, f):
        return type(self)([x.map_leaves(f) for x in self._parts])


class And(ConstituentCondition):
    pass

class Or(ConstituentCondition):
    pass

def or_condition(parts):
    if len(parts) == 1:
        return parts[0]
    elif len(parts) > 1:
        return Or(parts)
    else:
        raise ValueError("Must have at least one condition in or")


class BinaryCondition(Condition):
    def __init__(self, lhs, rhs):
        self._lhs = lhs
        self._rhs = rhs

    def __repr__(self):
        return "{}({!r}, {!r})".format(type(self).__name__, self._lhs, self._rhs)

    def __hash__(self):
        return hash((type(self), self.lhs, self.rhs))

    def __eq__(self, other):
        return (type(self) == type(other) and
                self.lhs == other.lhs and
                self.rhs == other.rhs)

    def copy(self):
        return type(self)(self._lhs, self._rhs)

    @property
    def lhs(self):
        return self._lhs

    @property
    def rhs(self):
        return self._rhs

    @property
    def fields(self):
        fields = []
        if isinstance(self.lhs, Field):
            fields = [self.lhs]
        return fields

class Equals(BinaryCondition):
    pass

class MatchesCond(BinaryCondition):
    pass

class GreaterThan(BinaryCondition):
    pass

class GreaterThanEqualTo(BinaryCondition):
    pass

class LessThan(BinaryCondition):
    pass

class LessThanEqualTo(BinaryCondition):
    pass

class GenericBinaryCondition(BinaryCondition):
    '''Generic binary condition, not data source specific

    Instances of BinaryCondition are created by the parsing condition
    strings. The data source converts these to more specific
    conditions

    '''
    def __init__(self, lhs, op, rhs):
        super(GenericBinaryCondition, self).__init__(lhs, rhs)
        self._op = op

    def copy(self):
        return GenericBinaryCondition(self._lhs, self._op, self._rhs)

    @property
    def op(self):
        return self._op

    def __repr__(self):
        return "GenericBinaryCondition({!r},{!r},{!r})".format(
            self.lhs, self.op, self.rhs
        )

    def __hash__(self):
        return hash((self._op, self.lhs, self.rhs)) 

    def __eq__(self, other):
        return (type(self) == type(other) and self.op == other.op
                and self.lhs == other.lhs and self.rhs == other.rhs)

