# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)
# - Paul Nilsson (paul.nilsson@cern.ch)


# add '--fileValidation=FALSE' to jobPars
def apply_mod(job_def):
    """

    :param job_def:
    :return:
    """

    jobpars = job_def['jobPars']

    jobpars += ' --fileValidation=FALSE'

    job_def['jobPars'] = jobpars

    return job_def
