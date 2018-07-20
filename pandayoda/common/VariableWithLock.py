from multiprocessing import Lock,Event,Value
import logging,ctypes
logger = logging.getLogger(__name__)


def get_array_type(var):
   if type(var) is int:
      return 'i'
   elif type(var) is float:
      return 'f'
   elif type(var) is long:
      return 'I'
   elif type(var) is str:
      if len(var) == 1:
         return 'c'
      else:
         return ctypes.c_char_p
   else:
      logger.error('invalid type object %s',str(var))


class VariableWithLock:
   def __init__(self,initial):
      
      self.value = Value(get_array_type(initial),initial,lock=True)

      self.is_set_flag = Event()

   def set(self,value):
      with self.value.get_lock():
         self.value.value = value
         self.is_set_flag.set()

   def get(self):
      return self.value.value

   def wait(self,timeout=None):
      self.is_set_flag.wait(timeout)

   def clear(self):
      self.is_set_flag.clear()


class VariableWithLock2:
   ''' this class represents a variable that when accessed, needs a
      Lock object set '''
   def __init__(self,initial=None):
      # the variable of interest
      self.variable = initial
      # the lock
      self.lock = Lock()
      # variable set event
      self.is_set_flag = Event()

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
