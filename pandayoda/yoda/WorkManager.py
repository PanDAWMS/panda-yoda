import os


class NoWorkLeft(Exception): pass

class WorkManager:
   def __init__(self):
      self.pandajobs = {}
      self.eventranges = {}
      self.workqueue = {}

   def add_pandajobs(self,pandajobs):
      self.pandajobs += pandajobs

   def add_eventranges(self,eventranges):
      self.eventranges += eventranges


   def get_work(self):
      if len(self.workqueue) > 0:
         pass
      else:
         raise NoWorkLeft('No Work Left on queue')

