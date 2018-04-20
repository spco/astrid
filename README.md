# Dependent Job Submission

This small Python 3 script solves the problem of how to submit many jobs to a job scheduler with multiple dependencies 
between jobs. Based on an input json file, the user defines which job dependencies, and the script submits each in the 
correct order.


### Known issues
Currently, if the graph of dependencies is acyclic (and thus unresolvable), the script will have submitted all the 
stages it can before aborting. Preferable default behaviour would be to not submit any jobs until the acyclicity is 
confirmed.  