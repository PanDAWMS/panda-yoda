

# removes '--DBRelease' from jobPars
def apply_mod(job_def):
   jobPars = job_def['jobPars']

   newPars = ''
   for par in jobPars.split():
      if not par.startswith('--DBRelease'):
         newPars += par + ' '

   job_def['jobPars'] = newPars
   return job_def
