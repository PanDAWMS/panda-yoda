#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - ..


# add '--fileValidation=FALSE' to jobPars
def apply_mod(job_def):
    """

    :param job_def:
    :return:
    """

    jobPars = job_def['jobPars']

    jobPars += ' --fileValidation=FALSE'

    job_def['jobPars'] = jobPars

    return job_def