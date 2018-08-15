

# increase athena message message limit
def apply_mod(job_def):
   jobPars = job_def['jobPars']
   if '--postExec' not in jobPars:
      jobPars += " --postExec 'svcMgr.MessageSvc.defaultLimit = 9999999;' "
   else:
      jobPars = jobPars.replace('--postExec ','--postExec "svcMgr.MessageSvc.defaultLimit = 9999999;" ')

   job_def['jobPars'] = jobPars

   return job_def
