import os,sys
from pandayoda.yoda import PandaJob

class PandaJobDict:
   ''' a list of PandaJob objects '''

   def __init__(self,pandajobs=None):
      ''' create a new list. Can pass a dictionary object that will be 
          parsed into PandaJob objects: {PandaID: {'PandaID': PandaID, ...}, ...}
      '''
      # list of PandaJob objects, keyed by PandaID
      self.jobs = {}

      if pandajobs:
         self.append_from_dict(pandajobs)

   def append_from_dict(self,pandajobs):
      for id,job in pandajobs.iteritems():
         self.jobs[str(id)] = PandaJob.PandaJob(job)

   def get_job_with_most_ready_events(self):
      # if there are no jobs, return None
      if len(self.jobs) == 0:
         return None
      # if there is only one job, return it if it has ready events
      elif len(self.jobs) == 1:
         job = self.jobs[self.jobs.keys()[0]]
         if job.events_ready() > 0:
            return job
         else:
            return None
      # if there are more than one job choose the one with the most events
      else:
         PandaID = self.jobid_with_most_ready_events()
         if PandaID > 0:
            return self.jobs[PandaID]
         # otherwise, return None
         else:
            return None

   def jobid_with_most_ready_events(self):
      max_id = 0
      max_ready = 0
      for jobid,job in self.jobs.iteritems():
         if job.events_ready() > max_ready:
            max_ready = job.events_ready()
            max_id = jobid

      return max_id

   def get_eventranges(self,pandaID):
      pandaID = str(pandaID)
      if pandaID in self.jobs.keys():
         return self.jobs[pandaID].eventranges
      return None

   def pop(self,key,default=None):
      return self.jobs.pop(key,default)
   def iteritems(self):
      return self.jobs.iteritems()
   def keys(self):
      return self.jobs.keys()
   def values(self):
      return self.jobs.values()
   def get(self,key,default=None):
      return self.jobs.get(key,default)
   def has_key(self,key):
      return self.jobs.has_key(key)

   def __iter__(self,key):
      return iter(self.jobs)
   def __len__(self):
      return len(self.jobs)
   def __getitem__(self,key):
      return self.jobs[key]
   def __setitem__(self,key,value):
      if isinstance(value,PandaJob.PandaJob):
         self.jobs[key] = value
      else:
         raise TypeError('object is not of type PandaJob: %s' % type(value).__name__)
   def __delitem__(self,key):
      del self.jobs[key]
   def __contains__(self,key):
      return self.jobs.__contains__(key)