import os,sys,threading,logging,shutil
from mpi4py import MPI
from pandayoda.common import MessageTypes,serializer,SerialQueue
logger = logging.getLogger(__name__)

class FileManager(threading.Thread):
   ''' FileManager: this thread accepts output from the JobComm to process AthenaMP event output files which need to be moved to local storage areas. '''
   def __init__(self,queues,
                output_file_path):
      ''' 
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send 
                     messages to other Droid components about errors, etc.
        outputFilePath: Where event files will be copied
        inputSerialQueueTimeout: A positive number of seconds to block when retrieving 
                     a message from the inputSerialQueue.'''
      # call base class init function
      super(FileManager,self).__init__()


      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # this is where event files will be copied
      self.output_file_path      = output_file_path

      # get current rank
      self.rank                  = MPI.COMM_WORLD.Get_rank()

      # the prelog is just a string to attach before each log message
      self.prelog                = 'Rank %03i:' % self.rank

      # this is used to trigger the thread exit
      self.exit = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the JobManager thread to exit'''
      self.exit.set()


   def run(self):
      ''' this is the function run as a subthread when the user runs fileManager_instance.start() '''

      while not self.exit.isSet():
         try:
            # this thread has nothing else to do so it can block on the message queue
            message = self.queues['FileManager'].get()
         except SerialQueue.Empty:
            pass
         else:
            ''' input message format: {'type':JobComm.OUTPUT_EVENT_FILE,
                         'filename':outputfilename,
                         'eventrangeid':eventrangeid,
                         'cpu':cpu,
                         'wallclock':wallclock
                         }
            '''

            if message['type'] == MessageTypes.OUTPUT_EVENT_FILE:
               if os.path.exists(message['filename']) and os.path.exists(self.output_file_path):
                  logger.debug('%s copying %s to %s',message['filename'],self.prelog,self.output_file_path)
                  shutil.copy(message['filename'],self.output_file_path + '/')
               elif not os.path.exists(message['filename']):
                  logger.error('%s input filename does not exist: %s',self.prelog,message['filename'])
               elif not os.path.exists(self.output_file_path):
                  logger.error('%s output file path does not exist: %s',self.prelog,self.output_file_path)

            else:
               logger.error('%s message type is incorrect: %s ',message['type'])

# testing this thread
if __name__ == '__main__':
   logging.basicConfig(level=logging.DEBUG,
         format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
         datefmt='%Y-%m-%d %H:%M:%S')
   logging.info('Start test of FileManager')
   import time
   #import argparse
   #oparser = argparse.ArgumentParser()
   #oparser.add_argument('-l','--jobWorkingDir', dest="jobWorkingDir", default=None, help="Job's working directory.",required=True)
   #args = oparser.parse_args()

   queues = {'FileManager':SerialQueue.SerialQueue()}

   fm = FileManager(queues,'/output/path')

   fm.start()

   while True:
      logger.info('start loop')
      if not fm.isAlive(): break

      msg = {
         'type':MessageTypes.OUTPUT_EVENT_FILE,
         'filename':'/build1/tsulaia/20.3.7.5/run-es/athenaMP-workers-AtlasG4Tf-sim/worker_1/myHITS.pool.root_000.Range-6',
         'eventrangeid':'ID:Range-6',
         'cpu':'CPU:1',
         'wallclock':'WALL:1' 
      }

      queues['FileManager'].put(msg)

      time.sleep(5)






   fm.stop()

   fm.join()

   logger.info('exiting test')


