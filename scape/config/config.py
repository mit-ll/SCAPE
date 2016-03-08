# Copyright (2016) Massachusetts Institute of Technology.  Reproduction/Use 
# of all or any part of this material shall acknowledge the MIT Lincoln 
# Laboratory as the source under the sponsorship of the US Air Force 
# Contract No. FA8721-05-C-0002.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''Configuration dictionary definition and creation

The Scape environment is configured through a combination of
enrionment variables and a JSON file which maps these environment
variables to actual system settings. This module handles the loading
of these variables into a Config dictionary.

The primary environment variable for Scape is SCAPE_CONFIG. This
should be a path pointing to the JSON file from which the environment
is configured.

'''

from __future__ import absolute_import
import ast
import abc
import json
import os
import sys
import glob
import logging as _logging
import traceback
from collections import OrderedDict
from datetime import datetime

from six import with_metaclass

import scape.utils
from scape.utils import (
    memoized, memoized_property,
)

from scape.config.errors import (
    ScapeConfigError, 
)

from scape.config.data import (
    ConfigData, 
)

from scape.config.logging import (
    ConfigLogging,
)

_log = scape.utils.new_log('scape.config.config')

DEFAULT_CONFIG_ENV_VAR = 'SCAPE_CONFIG'

def from_environment(env_var=DEFAULT_CONFIG_ENV_VAR):
    '''Load the JSON from the paths given in the SCAPE_CONFIG environment
    variable into a list of dictionaries.  Each path should point
    to a JSON-like object which will be interpretted as a Python
    dictionary.

    Returns: list of dictionaries

    '''
    dicts = []
    try:
        if env_var in os.environ:
            config_pathset = os.environ[env_var]
            paths = config_dpathset.split(os.pathsep)
            _log.debug('config paths: %s',paths)
            dicts = from_paths(paths)
        else:
            _log.error(
                "{} environment variable not in environment, " +
                "unable to load configuration".format(env_var)
            )

    except:
        _log.error(
            "Loading configuration from environment failed: \n"
            "  Error: \n"
            "{}".format(
                scape.utils.traceback_string(),
            )
        )

    return dicts

def from_paths(paths):
    '''Load the JSON from the given paths into a list of dictionaries.
    Each path should point to a JSON-like object which will be
    interpretted as a Python dictionary.

    Parameters:

    - paths (list of strings): list of paths from which to load
      the JSON configurations into dictionaries

    Returns: list of dictionaries

    '''
    dicts = []
    try:
        for path in paths if paths else []:
            if os.path.exists(path):
                D = scape.utils.read_json(path)
                if not isinstance(D,dict):
                    _log.error(
                        " Configuration file doesn't contain a"
                        " valid JSON object: {}".format(path)
                    )
                else:
                    dicts.append(D)
            else:
                _log.error(
                    "Configuration file doesn't exists: {}".format(path)
                )
    except:
        _log.error(
            "Loading configuration failed: \n"
            "  Path: {} \n"
            "  Error: \n"
            "{}".format(
                path, scape.utils.traceback_string(),
            )
        )
    return dicts

def expand_vars(config):
    '''Expand all environment variables referenced in Scape configuration

    Recurse through all key-value pairs in Scape configuration
    dictionary, expanding the environment variables referenced in the
    value (if it is a string).

    E.g. for some user "scapeuser" whose home path is "/home/scapeuser"

    {"some_scape_config": "$HOME/path/in/users/home"}

    would become

    {"some_scape_config": "/home/scapeuser/path/in/users/home"}

    '''
    stack = [config]
    while stack:
        D = stack.pop()
        for key,val in D.items():
            if type(val) in {list,tuple}:
                nval = list(val)
                for i,v in enumerate(val):
                    nval[i] = os.path.expandvars(v)
                if type(val) is tuple:
                    D[key] = tuple(nval)
                else:
                    D[key] = nval
            elif isinstance(val,dict):
                stack.append(val)
            elif type(val) in {str,unicode}:
                D[key] = os.path.expandvars(val)

def expand_registry(config):
    ''' Expand the registry object
    '''
    if 'registry' in config:
        if 'paths' in config['registry']:
            if config['registry']['paths']:
                P = config['registry']['paths'].split(os.pathsep)
                P = reduce(lambda x,y:x+y,
                           [glob.glob(p) for p in P],[])
                config['registry']['paths'] = P
            # Read in JSON objects from paths
            J = [scape.utils.read_json(p)
                 for p in config['registry']['paths']]
            # Merge the JSON objects into a single object
            config['registry']['object'] = reduce(
                scape.utils.merge_dicts,J,{}
            )

        if 'time_events' in config['registry']:
            time_events = config['registry']['time_events'].split(os.pathsep)
            config['registry']['time_events'] = time_events

class ConfigVars:
    root = "${HOME}/scape"
    home = os.path.join(root, "scape")
    user_home = "${HOME}/.scape"
    classes = [
        'scape.config.Config',
        'scape.config.data.ConfigData',
        "scape.config.logging.ConfigLogging",
    ]

class ConfigMeta(abc.ABCMeta):
    def __new__(cls, name, parents, dct):
        config = dct.setdefault('config',())
        cdict = OrderedDict()
        stack = [(config,cdict)]
        while stack:
            items = stack.pop()
            for name, value in items:
                
        return super(ConfigMeta,cls).__new__(cls,name,parents,dct)

class ConfigBase(with_metaclass(ConfigMeta,dict)):
    def __init__(self):
        


class Config(dict):
    '''Base class for Scape configuration dictionary

    This dictionary object represents the configuration settings for
    Scape. It has multiple means of setting up this configuration:
    shell environment-based, file-system based and in-memory.

    The purpose is to allow users to work with multiple configurations
    at once.

    ** TODO: think through **

    - Designed to be mixed with other subclasses of ConfigBase, each
      with its own convenience API for information based on
      configuration.

    - Trying to make Scape more extensible without being too
      cumbersome. Will need more thought.

    '''

    config = (
        ("root", root),
        ("home", home),
        ("user_home", user_home),
        ("classes", classes),
    )
    
    def __init__(self, paths=None, dicts=None):
        '''Constructor for Config dictionary

        Load the configuration information into memory using this
        process:

        1. Load the contents of the JSON paths provided by the
           environment variable SCAPE_CONFIG (if it exists) into
           master list of dictionaries

        2. Load the contents of the JSON paths provided by the `paths`
           parameter into master list of dictionaries

        3. Load the dictionaries provided by the dicts argument into
           master list of dictionaries

        Merge the master list of dictionaries into this Config
        dictionary.

        Arguments
        - paths (list of strings): list of paths of JSON configuration
          files
        - dicts (list of dictionaries): list of dictionaries of
          configuration information
        '''
        master_dicts = (
            self.from_dicts(from_environment()) +
            self.from_dicts(from_paths(paths)) +
            self.from_dicts(dicts)
        )

        self.init_from_dicts(master_dicts)

    @property
    def user_home(self):
        '''Path in user's home directory where Scape configuration
        information is stored (default is ~/.scape)

        '''
        home_dir = os.path.abspath(self['user_home'])
        return home_dir

    def init_from_dicts(self, dicts):
        '''Load this configuration object from the given list of
        dictionaries. The final configuration will be produced from
        successive merging of the dictionaries. Shell environment
        variables present in the values of any dictionary will be
        expanded, and the registry dictionary (if present) will be
        similarly expanded.

        Parameters:

        - dicts (list of dicts): list of dictionaries to merge into
          this configuration

        Returns: None

        '''
        try:
            scape.utils.merge_dicts_ip(self, dicts)
            self.expand_vars()
            self.expand_registry()
            # errors = self.init_environment()
            # if errors:
            #     raise ScapeConfigError("\n\n".join(
            #         ["** {}".format(e) for e in errors]
            #     ))
        except:
            _log.error(
                "Merging dictionaries failed: \n"
                "  Error: \n"
                "{}".format(
                    scape.utils.traceback_string(),
                )
            )

    def from_dicts(self, dicts):
        '''Verify that every dictionary in the given list of dicts is a valid
        configuration dictionary. If not, log and raise
        ScapeConfigError.

        Parameters:

        - dicts (list of dictionaries): list of configuration
          dictionaries to be verified

        Returns: list of dictionaries

        TODO:
        - Verification

        '''
        return list(dicts) if dicts else []

    def get(self,key,default={}):
        '''Same as normal dict object's get method, except that the default
        value is a dictionary (i.e. for chained get calls)

        '''
        return dict.get(self,key,default)

class ConfigInitializer(object):
    ''' Given a Scape Config dictionary, initialize the environment
    '''
    def __init__(self, config):
        self.config = config
    def init_environment(self):
        '''Initialize Scape environment, return errors (if any)

        Performs the following actions:
        - Creates user home (default: ~/.scape) if it doesn't exist
        - Confirms user home is writable

        '''
        errors = (
            self.create_user_home() +
            self.check_user_home()
        )
        return errors

    def create_user_home(self):
        ''' Create user home directory (~/.scape)

        Returns:
            List of error strings
        '''
        errors = []
        try:
            if not os.path.exists(self.config.user_home):
                os.makedirs(self.config.user_home)
        except OSError:
            errors.append(
                "Could not create SCAPE_USER_HOME: \n"
                "{}".format(
                    scape.utils.traceback_string(),
                )
            )

        return errors

    def check_user_home(self):
        ''' Confirms that user home is writable, returns errors if any

        Returns:
            List of error strings
        '''
        errors = []
        if not os.access(self.config.user_home, os.W_OK):
            errors.append("SCAPE_USER_HOME is not writable.")

        return errors
        

        
@memoized
def default_config(initialize=True):
    conf = Config()
    if initialize:
        errors = conf.init_environment()
        if errors:
            _log.error('\n'.join(errors))
    return conf

