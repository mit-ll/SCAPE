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
#!/usr/bin/python

#
# sge.py
#
# Creative Commons Attribution License
# http://creativecommons.org/licenses/by/2.5/
#
# Trevor Strohman
#    First release: December 2005
#    Second version:  11 January 2006
#
# Bug finders: Fernando Diaz 
#
# MITLL Version
# David O'Gwynn
#    First version:  18 April 2013
#

"""Submits jobs to Grid Engine.
Handles job dependencies, output redirection, and job script creation.
"""

from __future__ import absolute_import
import os
import shutil
import time
import StringIO
import logging
import xml.etree.cElementTree as cet

import dateutil.parser

import scape.utils

_log = logging.getLogger('scape.utils.sge')
_log.addHandler(logging.NullHandler())

MAXJOBS = 75000-1

def is_sge_process():
    V = set(os.environ)
    reqd = {'SGE_STDERR_PATH','SGE_STDOUT_PATH',
            'JOB_ID','JOB_NAME',
            'SGE_TASK_ID',
            }
    return V.issuperset(reqd)

class Job(object):
    def __init__( self, name, command, queue=None, job_dir=None ):
        self.name = name
        self.queue = queue
        self.command = command
        self.script = command
        self.job_dir = job_dir
        self.dependencies = []
        self.submitted = 0

    def addDependency( self, job ):
        self.dependencies.append( job )

    def wait( self ):
        finished = 0
        interval = 5
        while not finished:
            time.sleep(interval)
            interval = min( 2 * interval, 60 )
            finished = os.system( "qstat -j %s > /dev/null" % (self.name) )

class JobGroup(object):
    def __init__( self, name, command, queue=None, arguments=None,
                  maxTasks=None, pivot=True, job_dir=None,
                  delete_job_dir=True, ):
        self.name = name
        self.queue = queue
        self.command = command
        self.maxTasks = maxTasks
        self.job_dir = job_dir
        self.delete_job_dir = delete_job_dir
        self.dependencies = []
        self.submitted = 0
        self.arguments = arguments if arguments else {}
        # self.pivot: Arguments are a pivot table where each argument
        # key corresponds to a list of values (i.e. column in a pivot
        # table).  Should be dispatched one job per "row".
        self.pivot = pivot

        self.script = ''
        self.tasks = None
        
        self.generateScript()

    def generateScript( self ):
        # total number of jobs in this group
        total = 1
  
        # for now, SGE_TASK_ID becomes TASK_ID, but we base it at zero
        self.script += """let "TASK_ID=$SGE_TASK_ID - 1"\n"""
  
        # build the array definitions
        for key in self.arguments.keys():
            values = self.arguments[key]
            line = ("%s_ARRAY=( " % (key))
            for value in values:
                line += "\'"
                line += value
                line += "\' "
            line += " )\n"
            self.script += line
            if not self.pivot:
                total *= len(values)
        if self.pivot:
            total = max([len(v) for v in self.arguments.values()] or [total])
            
        self.script += "\n"

        # now, build the decoding logic in the script
        for key in self.arguments.keys():
            count = len(self.arguments[key])
            self.script += """let "{key}_INDEX=$TASK_ID % {count}"\n""".format(
                key=key, count=count,
                )
            self.script += """{key}=${{{key}_ARRAY[${key}_INDEX]}}\n""".format(
                key=key,
                )
            if not self.pivot:
                self.script += """let "TASK_ID=$TASK_ID / {}"\n""".format(count)
    
        # now, run the job
        self.script += "\n"
        self.script += self.command
        self.script += "\n"
  
        # set the number of tasks in this group
        self.tasks = total
  
    def addDependency( self, job ):
        self.dependencies.append( job )
  
    def wait( self ):
        finished = 0
        interval = 5
        while not finished:
            time.sleep(interval)
            interval = min( 2 * interval, 60 )
            finished = os.system( "qstat -j %s > /dev/null" % (self.name) )

def build_directories( directory ):
    if not os.path.exists(directory):
        os.makedirs(directory)
    subdirectories = [ "output", "stderr", "stdout", "jobs" ]
    directories = [ os.path.join( directory, subdir )
                    for subdir in subdirectories ]
    needed = [d for d in directories if not os.path.exists(d)]
    for d in needed:
        os.mkdir(d)

def build_job_scripts( directory, jlist ):
    for job in jlist:
        scriptPath = os.path.join( directory, "jobs", job.name )
        with open(scriptPath,'w') as scriptFile:
            scriptFile.write( "#!/usr/bin/env bash\n" )
            scriptFile.write( 'echo {} 1>&2\n'.format('START'.center(80,'-')) )
            scriptFile.write( 'echo "JOB INFO:" 1>&2\n' )
            scriptFile.write( 'echo "   ID: ${JOB_ID}" 1>&2\n' )
            scriptFile.write( 'echo " Name: ${JOB_NAME}" 1>&2\n' )
            scriptFile.write( 'echo " Host: `hostname`" 1>&2\n' )
            scriptFile.write( 'date 1>&2\n' )
            scriptFile.write( 'echo {} 1>&2\n'.format('-'*80 ))
            #scriptFile.write( "#$ -S /bin/bash\n" )
            scriptFile.write( job.script + "\n" )
            scriptFile.write( 'echo {} 1>&2\n'.format('-'*80 ))
            scriptFile.write( 'date 1>&2\n' )
            scriptFile.write( 'echo {} 1>&2\n'.format('END'.center(80,'-')) )

        os.chmod( scriptPath, 0755 )
        job.scriptPath = scriptPath

def extract_submittable_jobs( waiting ):
    """Return all jobs that aren't yet submitted, but have no
    dependencies that have not already been submitted."""
    submittable = []

    for job in waiting:
        unsatisfied = sum([(subjob.submitted==0)
                           for subjob in job.dependencies])
        if unsatisfied == 0:
            submittable.append( job )

    return submittable

def submit_safe_jobs( directory, jlist ):
    """Submits a list of jobs to Grid Engine, using the directory
    structure provided to store output from stderr and stdout."""
    for job in jlist:
        job.out = os.path.join( directory, "stdout" )
        job.err = os.path.join( directory, "stderr" )
  
        args = " -V -N %s " % (job.name)
        args += " -o %s -e %s " % (job.out, job.err)
  
        if job.queue != None:
            args += "-q %s " % job.queue
  
        if isinstance( job, JobGroup ):
            args += "-t 1:%d " % ( job.tasks )
            if job.maxTasks:
                args += '-tc %d ' % (job.maxTasks)
  
        if len(job.dependencies) > 0:
            args += "-hold_jid "
            for dep in job.dependencies:
                args += dep.name + ","
            args = args[:-1]
  
        qsubcmd = ("qsub %s %s" % (args, job.scriptPath)) 
        retcode = os.system( qsubcmd )
        _log.info('return code: %s',retcode)
        job.submitted = 1
    
def run_safe_jobs( directory, jlist ):
    """In the event that Grid Engine is not installed, this program will
    run jobs serially on the local host."""
    for job in jlist:
        job.out = os.path.join( directory, "stdout" )
        job.err = os.path.join( directory, "stderr" )

        commands = []
        if isinstance( job, JobGroup ):
            for task in range(1,job.tasks+1):
                command = "export SGE_TASK_ID=%d; %s" % (task, job.scriptPath)
                commands.append(command)
        else:
            commands.append(job.scriptPath)

        count = 0
        for command in commands:
            _log.info("# %s",command)
            command += " 2>%s/%s.%d >%s/%s.%d" % (job.err, job.name, count, job.out, job.name, count)
            os.system(command)
            count += 1
        job.submitted = 1
    
def check_for_qsub():
    for directory in os.environ['PATH'].split(':'):
        if os.path.isfile(os.path.join(directory, "qsub")):
            return True
    return False

def submit_jobs( directory, jlist, use_grid_engine=True ):
    waiting = list(jlist)
  
    while len(waiting) > 0:
        # extract submittable jobs
        submittable = extract_submittable_jobs( waiting )
    
        # run those jobs
        if use_grid_engine:
            submit_safe_jobs( directory, submittable )
        else:
            run_safe_jobs( directory, submittable )
    
        # remove those from the waiting list
        for j in submittable:
            waiting.remove(j)


def build_submission( directory, jlist ):
    # check to see if qsub exists
    use_grid_engine = check_for_qsub()

    delete = any({j.delete_job_dir for j in jlist})
    if delete:
        if os.path.exists(directory):
            _log.info('removing existing job directory: {}'.format(directory))
            shutil.rmtree(directory)
    
    # build all necessary directories
    build_directories( directory )
  
    # build job scripts
    build_job_scripts( directory, jlist )
  
    # submit the jobs
    submit_jobs( directory, jlist, use_grid_engine )

def launch_jobs(jlist):
    job_dirs = {j.job_dir for j in jlist}
    if len(job_dirs)==1:
        job_dir = tuple(job_dirs)[0]
        if job_dir is None:
            job_dir = os.path.abspath('./.SGE')
    else:
        if len(job_dirs-{None})==1:
            job_dir = tuple(job_dirs-{None})[0]
        else:
            raise AttributeError('Jobs submitted together must have the *sam* job directories: {}'.format(job_dirs))

    

    for j in jlist:
        if NAME_SEP not in j.name:
            j.name = generate_name(j.name,dir=job_dir)
        
    build_submission( job_dir, jlist )
        

NAME_SEP = '+++'
KV_SEP = '++'
NAME_PART_SEP = '_'
K_SEP = '~'
PATH_SEP = '+'
def generate_name(*name_parts, **options):
    name = NAME_PART_SEP.join([str(p) for p in name_parts])
    opstr = KV_SEP.join(['{}{}{}'.format(k,K_SEP,
                                         v.replace(os.path.sep,PATH_SEP))
                         for k,v in options.items()])
    return '{name}{sep}{opstr}'.format(
        name=name, opstr=opstr, sep=NAME_SEP,
        )

def parse_name(job_name):
    name,opstr = job_name.split(NAME_SEP,1)
    options = {}
    for op in opstr.split(KV_SEP):
        _,v = op.split(K_SEP)[:2]
        v = v.replace(PATH_SEP,os.path.sep)
    return name,options

def jobs():
    class job(dict):
        @property
        def number(self):
            return self.get('JB_job_number')
        @property
        def name(self):
            return self.get('JB_name')
        @property
        def owner(self):
            return self.get('JB_owner')
        @property
        def time(self):
            ts = self.get('JAT_start_time')
            if ts:
                return dateutil.parser.parse(ts)
        @property
        def hostname(self):
            qname = self.get('queue_name')
            if qname:
                return qname.split('@')[-1]
        @property
        def task(self):
            if 'tasks' in self:
                return self['tasks']
            
        def kill(self):
            jnum = self.number
            if jnum:
                scape.utils.shwatch('qdel {}'.format(jnum))

    o,_,_ = scape.utils.sh('qstat -xml',quiet=True)
    generator = cet.iterparse(StringIO.StringIO(o),events=('start','end'))
    main_job_info_node = None
    job_info_node = None
    queue_info_node = None
    jnodes = []

    for event,node in generator:
        if event=='start' and node.tag == 'job_info':
            if main_job_info_node is None:
                main_job_info_node = node
            else:
                job_info_node = node
        elif event=='start' and node.tag == 'queue_info':
            queue_info_node = node
        elif event=='end' and node.tag == 'queue_info':
            queue_info_node.clear()
            queue_info_node = None
        elif event=='end' and node.tag == 'job_info':
            if job_info_node:
                job_info_node.clear()
                job_info_node = None
        elif event=='end' and node.tag == 'job_list':
            job_list = node
            jdict = job({'jl_'+k:v for k,v in job_list.items()})
            for cnode in job_list:
                jdict[cnode.tag] = cnode.text
            jnodes.append(jdict)
            if queue_info_node:
                queue_info_node.clear()
            elif job_info_node:
                job_info_node.clear()
    return jnodes
