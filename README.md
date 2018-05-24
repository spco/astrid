# ASTRID - Automated Submission Tool foR jobs with InterDependencies

This small Python 3 script solves the problem of how to submit many jobs to a job scheduler with multiple dependencies 
between jobs. This can be a particular issue when a multi-stage pipeline requires separate scheduled jobs for each stage.
Based on an input json file, the user defines the job dependencies, and the script submits each in the correct order. 
ASTRID does not wait for jobs to launch or complete before submitting dependent jobs - it is left up to the job scheduler 
to schedule and launch the jobs, and to cancel any jobs dependent on failed jobs. 

Job dependencies are resolved before submission, to ensure that the full pipeline can be submitted.

Helpful error messages are output if the jobs cannot be submitted due to mutual dependencies.

## Usage

Call ASTRID with python3 astrid.py /name/of/input/file . The input file should be a JSON file with structure as given in 
input.json. Jobs are listed as keys, with their dependencies as comma-separated strings in the values. Jobs with no 
dependencies use the empty string "" as their value.

## Package dependencies

The following packages are required, and easily installable by your preferred package manager if they are not present 
in your installation: 
* argparse 
* json
* subprocess 
* numpy
* collections
 
