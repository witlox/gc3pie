#! /usr/bin/env python
#
#   gunpacker.py -- Front-end script for Extracting Zips/Tars into a storage system.
#
#   Copyright (C) 2017, 2018 S3IT, University of Zurich
#
#   This program is free software: you can redistribute it and/or
#   modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
"""
Front-end script for running multiple 'unarchive' jobs 
from an initial list archive documents.

It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gunpacker.py --help`` for program usage
instructions.
"""

# summary of user-visible changes
__changelog__ = """
  2017-07-06:
  * Initial version
"""
__author__ = 'Pim Witlox <pim.witlox@uzh.ch>'
__docformat__ = 'reStructuredText'
__version__ = '1.0'

# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gunpacker
    gunpacker.gunpackerScript().run()

import os
import tarfile
import zipfile

from pkg_resources import Requirement, resource_filename

from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript, existing_file, existing_directory


## Utility methods
def _get_archives(input_folder):
    """
    Returns list of valid .tar/.zip input files.
    """
    return [os.path.join(input_folder, valid_input_files)
            for valid_input_files in os.listdir(input_folder)
            if tarfile.is_tarfile(os.path.join(input_folder, valid_input_files))
            or zipfile.is_zipfile(os.path.join(input_folder, valid_input_files))]


## custom application class
class gunpackerApplication(Application):
    """
    Custom class to wrap the execution of the python scripts passed in src_dir.
    """
    application_name = 'gunpacker'
    
    def __init__(self, input_file, container, **extra_args):

        executables = []
        inputs = dict()

        with open('amaterial.dat', 'w') as fp:
            fp.write('auth={0}\n'.format(os.environ['OS_AUTH_URL']))
            fp.write('proj={0}\n'.format(os.environ['OS_PROJECT_NAME']))
            fp.write('user={0}\n'.format(os.environ['OS_USERNAME']))
            fp.write('sess={0}\n'.format(os.environ['OS_PASSWORD']))
        am_wrapper = resource_filename("amaterial.dat")
        inputs[am_wrapper] = "amaterial.dat"

        command = 'python unpack-swift.py -a {archive_file} -c {container} '
        if extra_args['requested_cores']:
            command += '-n {num_cores} '.format(num_cores=extra_args['requested_cores'])
        if extra_args['prefix']:
            command += '-p {prefix} '.format(prefix=extra_args['prefix'])

        if not extra_args['sharedFS']:
            inputs[input_file] = os.path.basename(input_file)
            cmd = command.format(archive_file=inputs[input_file], container=container)
        else:
            cmd = command.format(archive_file=input_file, container=container)
            
        if extra_args['gunpacker_script']:
            inputs[extra_args['gunpacker_script']] = "./unpack-swift.py"
        else:
            wrapper = resource_filename(Requirement.parse("gc3pie"), "gc3libs/etc/unpack-swift.py")
            inputs[wrapper] = "./unpack-swift.py"

        Application.__init__(
            self,
            arguments=cmd,
            inputs=inputs,
            stdout='gunpacker.log',
            join=True,
            executables=executables,
            **extra_args)


class gunpackerScript(SessionBasedScript):
    """
    The ``gunpacker`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gunpacker`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gunpacker``
    aggregates them into a single larger output file located in 
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version=__version__,
            application=gunpackerApplication,
            stats_only_for=gunpackerApplication,
            )

    def setup_options(self):
        self.add_param("-P", "--gunpacker", metavar="[PATH]",
                       type=existing_file,
                       dest="gunpacker_script", default=None,
                       help="Location of python script to process input twitter file. "
                            "Default: %(default)s.")
        self.add_param("-S", "--sharedfs", dest="shared_FS", 
                       action="store_true", default=True,
                       help="Whether the destination resource should assume shared filesystem "
                            "where Input/Output data will be made available. Data transfer will "
                            "happen through local filesystem. Default: %(default)s.")
        self.add_param("--cores", dest="core_count", default=4, help="Specify number of cores to "
                                                                     "use Default: %(default)s.")
        self.add_param("--prefix", help="Prefix of the root to write to.")

    def setup_args(self):        
        self.add_param('input_folder',
                       type=existing_directory,
                       help="Path to input folder containing valid input "
                            "twitter .tar/zip files. Does NOT navigate the folder.")
        self.add_param("container", help="Specify the Swift container to write to.")

    def new_tasks(self, extra):
        """
        For each valid input file create a new gunpackerRetryableTask
        """
        tasks = []
        
        for input_file in _get_archives(self.params.input_folder):
            job_name = os.path.basename(input_file)
                
            extra_args = extra.copy()
            extra_args['jobname'] = job_name
            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', job_name)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', job_name)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', job_name)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', job_name)
            extra_args['gunpacker_script'] = self.params.gunpacker_script
            extra_args['sharedFS'] = self.params.shared_FS
            extra_args['requested_cores'] = self.params.core_count
            extra_args['prefix'] = self.params.prefix

            self.log.debug("Creating Application for twitter data '{0}'".format(input_file))
            
            tasks.append(gunpackerApplication(input_file, self.params.container, **extra_args))
            
        return tasks
