import os
import multiprocessing

# running inside the Athena release, at least for 21.0.15,
# the python version can be strange. While it reports Python 2.7
# some behaviors are like 2.6.
# Especially in multiprocessing module
# This file tries to masks these differences by overridding the
# module objects if the environment variable 'RUN_YODA_IN_ATHENA'
# is set.


if 'RUN_YODA_IN_ATHENA' in os.environ:
   print('Running with custom multiprocessing module')
   from multiprocessing import synchronize
   # for the Event object, in 2.6 the Event.wait(timeout=None) function
   # always returns None
   # for Python 2.7+ it returns True if the internal flag is set
   # it returns False otherwise. However the Python version 2.7
   # that comes with Athena has the 2.6 behavior

   class Event(synchronize.Event):
      # overload the wait to behave like the 2.7+ version
      def wait(self,timeout=None):
         super(Event,self).wait(timeout)
         return self.is_set()

else:
   print('Running with standard multiprocessing module')
   Event = multiprocessing.Event

# no changes to these so just pass through
Process = multiprocessing.Process
Lock = multiprocessing.Lock
Queue = multiprocessing.Queue
Manager = multiprocessing.Manager
cpu_count = multiprocessing.cpu_count
Value = multiprocessing.Value
