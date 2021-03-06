import argparse
import json
import subprocess
import numpy as np
import collections
import re
import os
import time
# Reads in a json file. Each item in the json file is a job to submit. The contents are its name, and which jobs
# it depends upon. The program takes tdhe jobs, submits them to torque, and captures the jobIDs returned.
# These are then used as inputs for the jobs which are dependent.


def submit_job(stage, dependency_ids):
    if dependency_ids:
        command = 'qsub -W depend=afterok:' + ':'.join([str(item) for item in dependency_ids]) + ' ' + str(stage) + '.sub'
    else:
        command = 'qsub ' + str(stage) + '.sub'
    # Execute command in bash
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    jobid = str(int(re.search(b'[0-9]+', output).group(0)))
    if error is not None:
        print('Abort - something went wrong in submitting!')
        print('Stage name:', stage, 'dependent on', dependency_ids)
        exit(1)
    print('Submitting ' + str(stage) + ' with job id ' + str(jobid) + ', dependent on jobs', dependency_ids)
    return jobid


def submit_stage(stage, data, jobids):
    dependencies = [item.strip() for item in data[stage].split(",")]
    print('Dependencies of ' + str(stage) + ':', dependencies)
    # Check for the blank string as dependency
    if dependencies != ['']:
        dep_ids = [jobids[dep] for dep in dependencies]
    else:
        dep_ids = None
    # submit stage
    jobids[stage] = submit_job(stage, dep_ids)
    return


# Based on https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm
def topological_sort(data, matrix):
    print('Calculating dependency order and checking for cyclic dependencies...', end='')
    list_of_all_stages = [item for item in data]
    # L holds the sorted elements
    L = []
    # Q holds the nodes with no incoming edges
    Q = [stage for stage in data if data[stage] == '']
    while Q:
        # Take an element with no incoming nodes, and place it in the sorted queue.
        L.append(Q.pop(0))
        # get index of popped stage in matrix
        n = list_of_all_stages.index(L[-1])
        # loop over all nodes m, looking for those with an edge e from n to m
        for m in range(matrix.shape[0]):
            # Check whether this stage has a dependency on the popped stage
            if matrix[m, n] == 1:
                matrix[m, n] = 0
                # Check whether m has no other incoming edges
                if not matrix[m, :].any() == 1:
                    # Add to list of nodes with no incoming edges
                    Q.append(list_of_all_stages[m])

    if matrix.any():
        print(' Failed!\n#####################')
        print('Error: cyclic dependency identified. The following stages\' dependencies could not be fully resolved:')
        for index in range(matrix.shape[0]):
            if matrix[index, :].any() == 1:
                print('', list_of_all_stages[index])
        print('\nAt the point that we abort, we have the following elements sorted:')
        print(L)
        exit(1)
    else:
        print(' Done!')
        print('No cyclic dependencies. Ready to submit all the jobs!\n')
    return L


def create_matrix(data):
    print('\nCreating connectivity matrix...', end='')
    matrix = np.zeros((len(data), len(data)))
    list_of_labels = []
    for label in data:
        list_of_labels.append(label)

    for label in data:
        for item in [stage.strip() for stage in data[label].split(',')]:
            if item != '':
                matrix[list_of_labels.index(label), list_of_labels.index(item)] = 1
    print(' Done!')
    return matrix


def print_summary(jobids, ts):
    print('\n' + 'Job name'.ljust(20), 'Job ID')
    print('-' * 34)
    for item in ts:
        print(item.ljust(20), jobids[item])
    return


def astrid(args):
    json_data = open(args.input_file).read()

    data = json.loads(json_data, object_pairs_hook=collections.OrderedDict)

    print('Stages to be submitted:')
    print([stages for stages in data])

    matrix = create_matrix(data)
    ts = topological_sort(data, matrix)

    jobids = dict()
    for stage in ts:
        submit_stage(stage, data, jobids)
        time.sleep(args.delay)

    print('\nAll jobs submitted!')
    print_summary(jobids, ts)


def create_parser():
    parser = argparse.ArgumentParser(description='Submit jobs with inter-dependencies to a job scheduler.')
    parser.add_argument('input_file', type=str,
                        help='A string containing the filename of the input JSON file.')
    parser.add_argument('job-submission-directory', type=str,
                        help='The directory containing the job submission files to be used. These should be named '
                             'stage.sub for each stage reference in the input JSON file.',
                        nargs='?', default=os.getcwd())
    parser.add_argument('delay', type=int,
                        help='Time delay between job submission, as a courtesy to the job scheduler when submitting '
                             'a large number of jobs. Defaults to 0.1 seconds.',
                        nargs='?', default=0.1)
    return parser


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    astrid(args)
