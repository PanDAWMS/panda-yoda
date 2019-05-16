#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)


# removes '--DBRelease' from jobPars
def apply_mod(job_def):
    jobPars = job_def['jobPars']

    newPars = ''
    for par in jobPars.split():
        if not par.startswith('--DBRelease'):
            newPars += par + ' '

    job_def['jobPars'] = newPars
    return job_def
