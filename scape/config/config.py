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
from typing import *
from configparser import (
    ConfigParser, ExtendedInterpolation, InterpolationError
)
    

import scape.utils
from scape.utils import (
    memoized, memoized_property,
)

from scape.config.errors import (
    ScapeConfigError, 
)

_log = scape.utils.new_log('scape.config.config')

DEFAULT_CONFIG_ENV_VAR = 'SCAPE_CONFIG'

class ScapeConfigParser(ConfigParser):
    def __init__(self, *a, **kw):
        kw.setdefault('interpolation',ExtendedInterpolation())
        super().__init__(*a, **kw)
        
    def as_dict(self) -> dict:
        '''Represent ConfigParser as a dictionary

        http://stackoverflow.com/a/3220891/1869370

        '''
        D = OrderedDict()
        for sec_name in self.sections():
            D[sec_name] = OrderedDict(self.defaults())
            items = []
            for option in self.options(sec_name):
                try:
                    value = self.get(sec_name, option)
                except InterpolationError as err:
                    raw_value = self.get(sec_name, option, raw=True)
                    expanded = os.path.expandvars(raw_value)
                    self.set(sec_name, option, expanded)
                    value = self.get(sec_name, option)
                items.append((option, value))
            for key, value in items:
                D[sec_name][key] = value
        return D

def from_environment(env_var:str=DEFAULT_CONFIG_ENV_VAR) -> List[dict]:
    '''Load the configuration files from the paths given in the
    SCAPE_CONFIG environment variable into a list of dictionaries.

    Each path should point to a INI file which will be interpretted as
    a Python dictionary.

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

def from_paths(paths:List[str]) -> List[dict]:
    '''Load the config file from the given paths into a list of
    dictionaries.

    Each path should point to a conf/INI file which will be
    interpretted as a Python dictionary.

    Parameters:

    - paths: list of paths from which to load the INI configurations
      into dictionaries

    Returns: list of configuration dictionaries

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

def expand_vars(config:dict) -> None:
    '''Expand **in-place** all environment variables referenced in Scape
    configuration

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

def expand_registry(config:dict) -> None:
    ''' Expand the registry object **in-place**
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

class ConfigBase(ConfigParser):
    '''Base class of configuration helper classes.

    Each scape addon module (e.g. scape_accumulo) will have a
    configuration file associated with it. If this configuration file
    contains a class key, then this represents a config mixin class
    that will be mixed with the Scape Config object by the
    ConfigManager.

    These Config objects will contain the
    configuration information in the file. Furthermore, they will
    provide a number of convenience methods for using that
    configuration information.

    >>> config = scape.config.default_config()
    >>> config.accumulo['host']

    '''
    def __init__(self, *a, **kw):
        pass
        

    def as_dict(self) -> dict:
        '''Represent ConfigParser as a dictionary

        http://stackoverflow.com/a/3220891/1869370

        '''
        D = OrderedDict()
        for sec_name in self.sections():
            D[sec_name] = OrderedDict(self.defaults())
            items = []
            for option in self.options(sec_name):
                try:
                    value = self.get(sec_name, option)
                except InterpolationError as err:
                    raw_value = self.get(sec_name, option, raw=True)
                    expanded = os.path.expandvars(raw_value)
                    self.set(sec_name, option, expanded)
                    value = self.get(sec_name, option)
                items.append((option, value))
            for key, value in items:
                D[sec_name][key] = value
        return D


class ConfigManager(object):
    def __init__(self, paths:List[str]=None, dicts:List[dict]=None, **kw):
        master_dicts = (
            self.from_dicts(from_environment()) +
            self.from_dicts(from_paths(paths)) +
            self.from_dicts(dicts)
        )

        self.init_from_dicts(master_dicts)

    def init_from_dicts(self, dicts: List[dict]) -> None:
        '''Create a configuration object from the given list of
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
            expand_vars(self)
            expand_registry(self)
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

    def from_dicts(self, dicts: List[dict]) -> List[dict]:
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

    def init_environment(self) -> List[str]:
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

    def create_user_home(self) -> List[str]:
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

    def check_user_home(self) -> List[str]:
        ''' Confirms that user home is writable, returns errors if any

        Returns:
            List of error strings
        '''
        errors = []
        if not os.access(self.config.user_home, os.W_OK):
            errors.append("SCAPE_USER_HOME is not writable.")

        return errors
        

class Config(ConfigBase):
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
    
    def __init__(self, paths:List[str]=None, dicts:List[dict]=None, **kw):
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
        super().__init__()

        master_dicts = (
            self.from_dicts(from_environment()) +
            self.from_dicts(from_paths(paths)) +
            self.from_dicts(dicts)
        )

        self.init_from_dicts(master_dicts)

    @property
    def user_home(self) -> str:
        '''Path in user's home directory where Scape configuration
        information is stored (default is ~/.scape)

        '''
        home_dir = os.path.abspath(self['user_home'])
        return home_dir

    def init_from_dicts(self, dicts: List[dict]) -> None:
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
            expand_vars(self)
            expand_registry(self)
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

    def from_dicts(self, dicts: List[dict]) -> List[dict]:
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

    def get(self, key:str, default:dict={}) -> Union[str, dict]:
        '''Same as normal dict object's get method, except that the default
        value is a dictionary (i.e. for chained get calls)

        '''
        return dict.get(self, key, default)

class ConfigInitializer(object):
    ''' Given a Scape Config dictionary, initialize the environment
    '''
    def __init__(self, config: dict):
        self.config = config

    def init_environment(self) -> List[str]:
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

    def create_user_home(self) -> List[str]:
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

    def check_user_home(self) -> List[str]:
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

