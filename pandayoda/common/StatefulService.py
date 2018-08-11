import logging,time
from pandayoda.common import VariableWithLock
from pandayoda.common.yoda_multiprocessing import Process,Event
logger = logging.getLogger()



class StatefulService(Process):
   ''' this thread class should serve as a base class for
       all threads that run infinite loops with a state
       associated with them '''

   # a list of allowable states, should be overridden
   # by derived class
   STATES = []

   def __init__(self,loop_timeout=30):
      # call init of Thread class
      super(StatefulService,self).__init__()

      # current state of this process
      self.state                       = VariableWithLock.VariableWithLock('NOT_YET_DEFINED')
      self.state_time                  = VariableWithLock.VariableWithLock(time.clock())

      # this is used to trigger the thread exit
      self.exit                        = Event()

      # loop_timeout decided loop sleep times
      self.loop_timeout                = loop_timeout


   def stop(self):
      ''' this function can be called by outside threads to cause the JobManager thread to exit'''
      self.exit.set()
   
   def get_state(self):
      ''' return current state '''
      return self.state.get()

   def set_state(self,state):
      ''' set current state '''
      if state in self.STATES:
         self.state.set(state)
         self.state_time.set(time.clock())
      else:
         logger.error('tried to set state %s which is not supported',state)

   def in_state(self,state):
      ''' test if current in state '''
      if state == self.get_state():
         return True
      return False

   def state_lifetime(self):
      return time.clock() - self.state_time.get()

   def run(self):
      ''' run when obj.start() is called '''
      raise Exception('derived class should redefine this function')
