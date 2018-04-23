import argparse
import json
import subprocess
import numpy as np
# Reads in a json file. Each item in the json file is a job to submit. The contents are its name, and which jobs
# it depends upon. The program takes tdhe jobs, submits them to torque, and captures the jobIDs returned.
# These are then used as inputs for the jobs which are dependent.

y = 0


def submit_job(stage, dependency_ids):
    global y
    # increment the jobid - this is a mock part since I don't get anything back from echo but do from qsub
    y += 1
    if dependency_ids:
        command = 'echo "-W depend=afterok:' + ':'.join([str(item) for item in dependency_ids]) + '"'
    else:
        command = 'echo'
    print(command)
    # Execute command in bash
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    if error is not None:
        print("Abort - something went wrong in submitting!")
        print("Stage name:", stage, "dependent on", dependency_ids)
        exit(1)
    print("submitting " + str(stage) + " with job id " + str(y) + ", dependent on jobs", dependency_ids)
    return y


def submit_stage(stage, data, jobids):
    dependencies = data[stage].split(",")
    print('dependencies:', dependencies)
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
    list_of_all_stages = [item for item in data]
    # L holds the sorted elements
    L = []
    # Q holds the nodes with no incoming edges
    Q = [stage for stage in data if data[stage] == '']
    print("Q:", Q, "L:", L)
    while Q:
        # Take an element with no incoming nodes, and place it in the sorted queue.
        L.append(Q.pop(0))
        print("Q:", Q, "L:", L)
        # get index of popped stage in matrix
        n = list_of_all_stages.index(L[-1])
        print("si:", n)
        # loop over all nodes m, looking for those with an edge e from n to m
        for m in range(matrix.shape[0]):
            print("m=", m)
            # Check whether this stage has a dependency on the popped stage
            if matrix[m, n] == 1:
                print("Edge found: matrix[m, n] == 1")
                matrix[m, n] = 0
                # Check whether m has no other incoming edges
                if not matrix[m,:].any() == 1:
                    # if list_of_all_stages[m] not in Q and list_of_all_stages[m] not in L:
                        # Add to list of nodes with no incoming edges
                    Q.append(list_of_all_stages[m])
        print(matrix)
        print("Q:", Q, "L:", L)

    print(not matrix.any())
    print(matrix)
    if matrix.any():
        print("Error: cyclic dependency identified. The following stages' dependencies could not be fully resolved:")
        for index in range(matrix.shape[0]):
            if matrix[index,:].any() == 1:
                print(list_of_all_stages[index])
        exit(1)
    else:
        print("No cyclic dependencies. Good to go!")
    return L


def create_matrix(data):
    matrix = np.zeros((len(data), len(data)))
    print(matrix)
    list_of_labels = []
    for label in data:
        list_of_labels.append(label)

    for i, label in enumerate(data):
        for item in data[label].split(','):
            if item != "":
                matrix[list_of_labels.index(label), list_of_labels.index(item)] = 1
    print(matrix)
    return matrix


def print_summary(jobids):
    print('\n' + 'Job name'.ljust(20), 'Job ID')
    print('-' * 34)
    for item in jobids:
        print(item.ljust(20), jobids[item])
    return


def djs(args):
    json_data = open(args.input_file).read()

    data = json.loads(json_data)

    list_of_stages = [stages for stages in data]
    print("Stages to be submitted:")
    print(list_of_stages)

    matrix = create_matrix(data)
    ts = topological_sort(data, matrix)

    jobids = dict()
    for stage in ts:
        submit_stage(stage, data, jobids)

    print("\nAll jobs submitted!")
    print_summary(jobids)


def create_parser():
    parser = argparse.ArgumentParser(description='Submit jobs with inter-dependencies to a job scheduler.')
    parser.add_argument('input_file', type=str,
                        help='A string containing the filename of the input JSON file.')
    return parser


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    djs(args)
