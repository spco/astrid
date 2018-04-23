# Dependent Job Submission

This small Python 3 script solves the problem of how to submit many jobs to a job scheduler with multiple dependencies 
between jobs. Based on an input json file, the user defines which job dependencies, and the script submits each in the 
correct order. DJS does not wait for jobs to launch or complete before submitting dependent jobs. It is left up to the 
job scheduler to schedule and launch the jobs, and to cancel any jobs dependent on failed jobs. 

Job dependencies are resolved before submission, to ensure that the full pipeline can be submitted.

Helpful error messages are output if the jobs cannot be submitted due to mutual dependencies.