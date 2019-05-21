# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)


# when jobPars has very long '--inputEVNTFiles' list
# it can be good for AthenaMP to remove all but one
def apply_mod(job_def):

    jobPars = job_def['jobPars']
    inFiles = job_def['inFiles']

    input_files = inFiles.split(',')

    start_index = jobPars.find('--inputEVNTFile=') + len('--inputEVNTFile=')
    end_index = jobPars.find(' ', start_index)

    jobPars = jobPars[:start_index] + input_files[0] + ' ' + jobPars[end_index:]

    job_def['jobPars'] = jobPars

    return job_def
   