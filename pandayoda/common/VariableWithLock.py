import threading

class VariableWithLock:
   ''' this class represents a variable that when accessed, needs a 
       threading.Lock object set '''
   def __init__(self,initial=None):
      # the variable of interest
      self.variable = initial
      # the lock
      self.lock = threading.Lock()

   def set(self,value):
      self.lock.acquire()
      self.variable = value
      self.lock.release()

   def get(self):
      value = None
      self.lock.acquire()
      value = self.variable
      self.lock.release()
      return value