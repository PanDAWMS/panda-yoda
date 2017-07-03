import logging,os,sys,importlib
logger = logging.getLogger(__name__)
from pandayoda.common import StatefulService,VariableWithLock
import DroidRequest

class DroidRequestList:
   ''' a list of DroidRequest threads to keep track of multiple requests '''

   def __init__(self):

      # internal list of droid requests
      self.droid_requests = []

   def add_request(self,config,loop_timeout=30):
      new_request = DroidRequest.DroidRequest(config,loop_timeout)
      new_request.start()
      self.append(new_request)

   def number_isAlive(self):
      counter = 0
      for req in self.droid_requests:
         if req.isAlive():
            counter += 1
      return counter

   def get_alive(self):
      reqs = []
      for req in self.droid_requests:
         if req.isAlive():
            reqs.append(req)
      return reqs

   def get_running(self):
      running = []
      for req in self.droid_requests:
         if req.running():
            running.append(req)
      return running

   def get_waiting(self):
      reqs = []
      for req in self.droid_requests:
         if req.get_state() == DroidRequest.DroidRequest.MESSAGE_RECEIVED:
            reqs.append(req)
      return reqs

   def number_running(self):
      running = 0
      for req in self.droid_requests:
         if req.running():
            running += 1
      return running

   def number_waiting_for_droid_message(self):
      waiting = 0
      for req in self.droid_requests:
         if not req.received_droid_message():
            waiting += 1
      return waiting

   def number_in_state(self,state):
      counter = 0
      if state in DroidRequest.DroidRequest.STATES:
         for req in self.droid_requests:
            if req.get_state() == state:
               counter += 1
      return counter

   def number_created(self):
      return self.number_in_state(DroidRequest.DroidRequest.CREATED)
   def number_requesting(self):
      return self.number_in_state(DroidRequest.DroidRequest.REQUESTING)
   def number_message_received(self):
      return self.number_in_state(DroidRequest.DroidRequest.MESSAGE_RECEIVED)
   def number_exited(self):
      return self.number_in_state(DroidRequest.DroidRequest.EXITED)
   
   def number_received_response(self):
      counter = 0
      for req in self.droid_requests:
         state = req.get_state()
         if state == DroidRequest.DroidRequest.RECEIVED_EVENTRANGES or state == DroidRequest.DroidRequest.RECEIVED_JOB:
            counter += 1

      return counter



   def append(self,request):
      if isinstance(request,DroidRequest.DroidRequest):
         self.droid_requests.append(request)
      else:
         raise TypeError('object is not of type DroidRequest: %s' % type(request).__name__)
   def insert(self,index,request):
      if isinstance(request,DroidRequest.DroidRequest):
         self.droid_requests.insert(index,request)
      else:
         raise TypeError('object is not of type DroidRequest: %s' % type(request).__name__)
   def pop(self,index=None):
      return self.droid_requests.pop(index)
   def index(self,value):
      return self.droid_requests.index(value)
   def remove(self,value):
      self.droid_requests.remove(value)
   def __iter__(self):
      return iter(self.droid_requests)
   def __len__(self):
      return len(self.droid_requests)
   def __getitem__(self,index):
      return self.droid_requests[index]
   def __setitem__(self,index,value):
      if isinstance(value,DroidRequest.DroidRequest):
         self.droid_requests[index] = value
      else:
         raise TypeError('object is not of type DroidRequest: %s' % type(value).__name__)
   def __delitem__(self,index):
      del self.droid_requests[index]
   def __contains__(self,value):
      return self.droid_requests.__contains__(value)

