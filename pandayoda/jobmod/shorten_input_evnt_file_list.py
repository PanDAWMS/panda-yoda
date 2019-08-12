# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)
# - Paul Nilsson (paul.nilsson@cern.ch)


# when jobPars has very long '--inputEVNTFiles' list
# it can be good for AthenaMP to remove all but one
def apply_mod(job_def):

    jobpars = job_def['jobPars']
    infiles = job_def['inFiles']

    input_files = infiles.split(',')

    start_index = jobpars.find('--inputEVNTFile=') + len('--inputEVNTFile=')
    end_index = jobpars.find(' ', start_index)

    jobpars = jobpars[:start_index] + input_files[0] + ' ' + jobpars[end_index:]

    job_def['jobPars'] = jobpars

    return job_def
