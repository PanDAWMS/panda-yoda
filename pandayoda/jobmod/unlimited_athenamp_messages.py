# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)


# increase athena message message limit
def apply_mod(job_def):
    jobPars = job_def['jobPars']
    if '--postExec' not in jobPars:
        jobPars += " --postExec 'svcMgr.MessageSvc.defaultLimit = 9999999;' "
    else:
        jobPars = jobPars.replace('--postExec ', '--postExec "svcMgr.MessageSvc.defaultLimit = 9999999;" ')

    job_def['jobPars'] = jobPars

    return job_def
