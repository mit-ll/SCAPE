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
''' scape.utils.shell

Utilities for dealing with OS shells
'''
from subprocess import Popen,PIPE,STDOUT

from scape.utils import lines as llines
from scape.utils.log import new_log

_log = new_log('scape.utils.shell')

class PipeShim(object):
    pass

def sh(command,content=None,quiet=False):
    '''Run blocking shell command (possibly with initial input to stdin)
    and return its output

    Suitable for short-running processes with relatively small amounts
    of output

    >>> output, error, pipe = sh('ls')
    >>> print output
    file0
    file1
    dir0
    >>> print error

    >>> print pipe.returncode
    0
    >>> output, error, pipe = sh('ls bad_dir')
    >>> print output

    >>> print error
    ls: bad_dir: No such file or directory
    >>> print pipe.returncode
    1

    '''
    if not quiet:
        _log.info(llines(command))
    pipe = Popen(
        command,stdin=PIPE,stdout=PIPE,stderr=PIPE,
        shell=True,universal_newlines=True,
        )
    if content:
        if type(content) is not bytes:
            content = bytes(content,'utf-8')
        out,err = pipe.communicate(content)
    else:
        out,err = pipe.communicate()

    pipe.wait()
    return out,err,pipe

def shwatch(command):
    '''Run blocking shell command *without input*, log lines of output
    during run and return the subprocess returncode and the lines
    gathered from stdout

    Suitable for medium-running processes with relatively small
    amounts of output that require observation during their run

    To see output line logging, the log level must be set to INFO.

    >>> import scape.config.logging
    >>> scape.config.logging.setup_logging('info')
    >>> retcode,lines = shwatch('ls')
    2014-08-22 09:36:07,398 [ INFO:scape.utils.shell ] (shell.py 87) : 
    file0
    2014-08-22 09:36:07,398 [ INFO:scape.utils.shell ] (shell.py 87) : 
    file1
    2014-08-22 09:36:07,398 [ INFO:scape.utils.shell ] (shell.py 87) : 
    dir0
    >>> len(lines)
    3
    >>> retcode
    0
    >>>

    '''
    pipe = Popen(
        command,stdin=PIPE,stdout=PIPE,
        stderr=STDOUT,
        #stderr=PIPE,
        bufsize=1,
        shell=True,universal_newlines=True,
        )

    all_lines = []
    while pipe.poll() is None:
        line = pipe.stdout.readline()
        if line:
            all_lines.append(line)
            _log.info(llines(line.strip()))

    lines = pipe.stdout.readlines()
    if lines:
        all_lines.extend(lines)
        for line in lines:
            _log.info(llines(line.strip()))
            
    returncode = pipe.wait()
    return returncode,all_lines

def shiter(command):
    '''Run non-blocking shell command *without input* and return a
    generator for lines from stdout

    Suitable for long-running processes whose outputs need to be
    processed as a stream of lines

    >>> ips = set()
    >>> def get_ips(line):
    ...     ips.update([ip.strip() for ip in line.split('\t')])
    ...
    >>> command = 'tshark -T fields -e ip.src -e ip.dst -r traffic.pcap'
    >>> for line in shiter(command):
    ...     get_ips(line)
    ...
    >>>

    '''
    pipe = Popen(
        command,stdin=PIPE,stdout=PIPE,
        stderr=PIPE,
        #stderr=PIPE,
        bufsize=1,
        shell=True,universal_newlines=True,
        )
    all_lines = []
    while pipe.poll() is None:
        line = pipe.stdout.readline()
        if line:
            yield line
    lines = pipe.stdout.readlines()
    if lines:
        for line in lines:
            yield line

