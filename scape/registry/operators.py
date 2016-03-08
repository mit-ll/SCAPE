"""
Copyright (2016) Massachusetts Institute of Technology.  Reproduction/Use 
of all or any part of this material shall acknowledge the MIT Lincoln 
Laboratory as the source under the sponsorship of the US Air Force 
Contract No. FA8721-05-C-0002.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
import abc
import collections
from six import with_metaclass

from scape.utils.log import new_log
from scape.utils.decorators import (
    memoized_property
)

_log = new_log('scape.registry.operators')

class RegistryOperatorMeta(abc.ABCMeta):
    def __new__(cls, name, parents, dct):
        if 'operator_class' in dct:
            if isinstance(dct['operator_class'],type):
                # Add the operator's docstring to this class's docstring
                # (if it exists)
                doc = cls.__doc__
                opdoc = dct['operator_class'].__doc__
                if opdoc is not None:
                    dct['__doc__'] = opdoc + (doc if doc else '')

        args = dct.get('args',())
        new_args = []
        arg_docs = []
        for i,arg_info in enumerate(args):
            attr,default,required,doc,choices = None,None,False,'',None
            if isinstance(arg_info,basestring):
                attr = arg_info
            else:
                if len(arg_info) == 1:
                    attr = arg_info[0]
                elif len(arg_info) == 2:
                    attr, default = arg_info
                    dct[attr] = default
                elif len(arg_info) == 3:
                    attr, default, doc = arg_info
                    dct[attr] = default
                elif len(arg_info) == 4:
                    attr, default, doc, choices = arg_info
            if default is None:
                required = True
            if doc:
                if required:
                    dval = '[REQUIRED]'
                else:
                    dval = "[default: {}]".format(default)
                cval = ''
                if choices:
                    cval = "[choices: {{{}}}]".format(', '.join(choices))
                arg_docs.append(
                    '({0}) {1} {2} {3}'.format(attr,doc,dval,cval)
                )
            new_args.append({
                'name':attr, 'required':required, 'index':i,
                'doc':doc, 'default':default, 'choices': choices,
            })

        dct['args'] = new_args
        dct['_args_lut'] = {a['name']:a for a in new_args}

        required_args = [a['name'] for a in new_args if a['required']]
        dct['required_args'] = required_args

        if arg_docs:
            docstring = dct.get('__doc__') or cls.__doc__ or ''
            docstring += '\nArgs:\n {}'.format('\n '.join(arg_docs))
            dct['__doc__'] = docstring

        return super(RegistryOperatorMeta,cls).__new__(cls, name, parents, dct)

class RegistryOperator(with_metaclass(RegistryOperatorMeta,
                                      collections.Iterator)):
    '''Handles operator functions for tagged dimensions over some Registry
    Selection

    '''
    # args is defined in subclasses as tuple (to specify ordering)
    # e.g.
    # args = (
    #    ("name0", ),   
    #    ("name1", "default_value1"), 
    #    ("name2", "default_value2", "documentaion2"), 
    #    ("name3", "default_value3", "documentaion3", ("choices3","b")),
    # )
    #
    # If a default value is not given (or is None), then the argument
    # is required.
    args = ()

    # _args_lut: lookup table for argument information [private]
    #    {argument_name: {'name': argument name,
    #                     'required': boolean,
    #                     'index': index in positional arg list,
    #                     'doc': documentation,
    #                     'default': default value,
    #                     'choices': sequence/set of valid choices }
    # 
    _args_lut = {}

    required_args = []

    tagged_dimensions = []

    selection = None
    
    def __init__(self):
        for arg in self.args:
            if arg['default'] is not None:
                setattr(self,arg['name'],arg['default'])

    @classmethod
    def check_args(cls, *pos, **kw):
        args,values = ((zip(*zip([a['name'] for a in cls.args],pos)) +
                        zip(*kw.items()))
                       or
                       ([],[]))

        # Check for required args
        if not set(args).issuperset(cls.required_args):
            missing = sorted(set(cls.required_args) - set(args),
                             key=lambda m:cls._args_lut[m]['index'])
            raise TypeError(
                "Missing arguments: {}".format(', '.join(
                    ['({0}) {1}'.format(m,cls._args_lut[m]['doc'])
                     for m in missing]
                ))
            )

        # Check for choice errors
        choice_errors = []
        for arg,value in zip(args,values):
            A = cls._args_lut[arg]
            if A['choices']:
                if not value in A['choices']:
                    choice_errors.append([arg,value,A['choices']])

        if choice_errors:
            err = 'Some of your arguments are not valid:'
            for arg,value,choices in choice_errors:
                err += '\n {0}: {1} (valid: [{2}])'.format(
                    arg, value, ', '.join(map(str,choices))
                )
            raise TypeError(err)

        return {a:v for a,v in zip(args,values)}
        

    def __call__(self, *a, **kw):
        new = self.__class__()
        args = new.check_args(*a,**kw)
        for arg in new.args:
            if arg['required'] or arg['name'] in args:
                setattr(new,arg['name'],args[arg['name']])
        
        return new

    def copy(self):
        args = {a['name']:getattr(self,a['name']) for a in self.args}
        return self(**args)

    @memoized_property
    def generator(self):
        raise NotImplementedError('Must be implemented in subclass')

    def next(self):
        for value in self.generator:
            yield value

    def __or__(self, operator):
        if not isinstance(operator, RegistryOperator):
            return NotImplemented

        return OperatorSequence(self,operator)

    @property
    def tabular_operators(self):
        return {}
        
class OperatorSequence(RegistryOperator):
    def __init__(self,*operators):
        super(OperatorSequence,self).__init__()
        self.operator_sequence = []
        for op in operators:
            if isinstance(op,OperatorSequence):
                self.operator_sequence.extend(op.operator_sequence)
            else:
                self.operator_sequence.append(op.copy())

    @memoized_property
    def generator(self):
        if self.selection:
            pass
        else:
            raise AttributeError('RegistryOperators must have a selection associate with them prior to iteration')

        
__all__ = []

