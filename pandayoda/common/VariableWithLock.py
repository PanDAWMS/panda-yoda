import threading

class VariableWithLock:
   ''' this class represents a variable that when accessed, needs a 
       threading.Lock object set '''
   def __init__(self,initial=None):
      # the variable of interest
      self.variable = initial
      # the lock
      self.lock = threading.Lock()
      # variable set event
      self.is_set_flag = threading.Event()

   def set(self,value):
      self.lock.acquire()
      self.variable = value
      self.is_set_flag.set()
      self.lock.release()

   def get(self):
      value = None
      self.lock.acquire()
      value = self.variable
      self.lock.release()
      return value

   def wait(self,timeout=None):
      self.is_set_flag.wait(timeout)
   def clear(self):
      self.is_set_flag.clear()