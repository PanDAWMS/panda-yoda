import os,sys,threading,logging,shutil
from pandayoda.common import MessageTypes,serializer,SerialQueue,yoda_droid_messenger as ydm
from mpi4py import MPI
logger = logging.getLogger(__name__)

class DroidComm(threading.Thread):
   ''' Droid Comm: this thread manages the communication with Droid '''
   def __init__(self,queues,
                output_file_path,
                loopTimeout      = 30):
      ''' 
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send 
                     messages to other Droid components about errors, etc.
        outputFilePath: Where event files will be copied
        inputQueueTimeout: A positive number of seconds to block when retrieving 
                     a message from the inputQueue.'''
      # call base class init function
      super(DroidComm,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # the timeout duration 
      self.loopTimeout           = loopTimeout

      # get current rank
      self.rank                  = MPI.COMM_WORLD.Get_rank()

      # the prelog is just a string to attach before each log message
      self.prelog                = 'Rank %03i:' % self.rank

      # this is used to trigger the thread exit
      self.exit = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the DroidComm thread to exit'''
      self.exit.set()
