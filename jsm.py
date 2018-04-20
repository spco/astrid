import argparse
import json

# Reads in a json file. Each item in the json file is a job to submit. The contents are its name, and which jobs
# it depends upon. The program takes the jobs, submits them to torque, and captures the jobIDs returned.
# These are then used as inputs for the jobs which are dependent.

x = 0


def submit_job_and_return_next_jobid(job_name, graph, jobids):
    global x
    x += 1
    labels = [item[0] for item in graph[:]]
    dependencies = [item for item in [subitem[0] for subitem in graph[:]]
                    if graph[labels.index(job_name)][labels.index(item)+1] == 1]
    print("submitted " + job_name + " as job id " + str(x) + ", dependent on jobs ", [(dep, jobids[dep]) for dep in dependencies])
    return x


def submit_jobs(graph, data):
    submitted_jobids = dict([])
    for label in data:
        submitted_jobids[label] = submit_job_and_return_next_jobid(label, graph, submitted_jobids)

    print(submitted_jobids)
    return 0


# Based upon the inputted json, create a directed graph of job dependencies.
def generate_graph(data):
    twodarray = []
    list_of_labels = []
    blank_list = [[] for _ in range(len(data))]
    for label in data:
        list_of_labels.append(label)
        twodarray.append([label] + blank_list)
        if data[label]["depends_on"] == "":
            print(label + " is root node")

    for i,label in enumerate(data):
        # print(label, data[label]['depends_on'].split(','))
        for item in data[label]['depends_on'].split(','):
            if item != "":
                twodarray[list_of_labels.index(label)][list_of_labels.index(item)+1] = 1

    print("twodarray:", twodarray)
    print("labels:", list_of_labels)
    return twodarray


def jsm(args):
    print("hello")
    json_data = open(args.input_file).read()

    data = json.loads(json_data)
    print(data)

    list_of_labels = [label for label in data]
    
    print(list_of_labels)

    graph = generate_graph(data)

    submit_jobs(graph, data)
    data["trim"]["jobID"] = "returnedjobID"


def create_parser():
    parser = argparse.ArgumentParser(description='Run the IASI native (L1C and L2) to NetCDF conversion process.')
    parser.add_argument('input_file', type=str,
                        help='A string in containing the filename of the JSON file to read in from.')
    return parser


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    jsm(args)
