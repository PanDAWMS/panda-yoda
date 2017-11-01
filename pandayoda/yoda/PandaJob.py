import os,sys
from pandayoda.common import EventRangeList

class PandaJob:
   ''' holds a panda job definition and its corresponding list of event ranges '''

   def __init__(self,job_def):

      self.job_def         = job_def

      self.eventranges     = EventRangeList()

   def events_ready(self):
      return self.eventranges.number_ready()

   def __str__(self):
      return str(self.job_def)

   def __iter__(self,key):
      return iter(self.job_def)
   def __len__(self):
      return len(self.job_def)
   def __getitem__(self,key):
      return self.job_def[key]
   def __setitem__(self,key,value):
      self.job_def[key] = value
   def __delitem__(self,key):
      del self.job_def[key]
   def __contains__(self,key):
      return self.job_def.__contains__(key)
