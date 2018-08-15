

# add '--fileValidation=FALSE' to jobPars
def apply_mod(job_def):
   jobPars = job_def['jobPars']

   jobPars += ' --fileValidation=FALSE'

   job_def['jobPars'] = jobPars

   return job_def