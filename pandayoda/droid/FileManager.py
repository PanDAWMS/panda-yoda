import os,sys,threading,logging,shutil
from mpi4py import MPI
from pandayoda.common import MessageTypes,serializer,SerialQueue
from pandayoda.common import yoda_droid_messenger as ydm
logger = logging.getLogger(__name__)


config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]

class FileManager(threading.Thread):
   ''' FileManager: this thread accepts output from the JobComm to process AthenaMP event output files which need to be moved to local storage areas. '''
   def __init__(self,config,queues,yoda_working_path):
      ''' 
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send 
                     messages to other Droid components about errors, etc.
        config: the ConfigParser handle for yoda
        yoda_working_path: the path where yoda is running
        droid_working_path: the path where this droid rank is running, and also AthenaMP
        '''
      # call base class init function
      super(FileManager,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # configuration of Yoda
      self.config                = config

      # the path where yoda is running
      self.yoda_working_path     = yoda_working_path

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
      if self.rank == 0:
         logger.debug('%s config_section: %s',self.prelog,config_section)

      # get loop_timeout
      if self.config.has_option(config_section,'loop_timeout'):
         loop_timeout = self.config.getfloat(config_section,'loop_timeout')
      else:
         logger.error('%s must specify "loop_timeout" in "%s" section of config file',self.prelog,config_section)
         return
      logger.info('%s %s loop_timeout: %d',self.prelog,config_section,loop_timeout)


      while not self.exit.isSet():
         try:
            # this thread has nothing else to do so it can block on the message queue
            message = self.queues['FileManager'].get(timeout=loop_timeout)
         except SerialQueue.Empty:
            pass
         else:
            ''' input message format: {'type':JobComm.OUTPUT_EVENT_FILE,
                         'filename':outputfilename,
                         'eventrangeid':eventrangeid,
                         'cpu':cpu,
                         'wallclock':wallclock
                         'scope':scopeOut
                         }
            '''

            if message['type'] == MessageTypes.OUTPUT_FILE:
               if os.path.exists(message['filename']) and os.path.exists(self.yoda_working_path):
                  
                  logger.debug('%s copying %s to %s',self.prelog,message['filename'],self.yoda_working_path)
                  shutil.copy(message['filename'],self.yoda_working_path)
               elif not os.path.exists(message['filename']):
                  logger.error('%s input filename does not exist: %s',self.prelog,message['filename'])
               elif not os.path.exists(self.yoda_working_path):
                  logger.error('%s output file path does not exist: %s',self.prelog,self.yoda_working_path)
            elif message['type'] == MessageTypes.WALLCLOCK_EXPIRING:
               logger.debug('%s received wallclock expiring message',self.prelog)
               self.stop()
               break
            else:
               logger.error('%s message type is incorrect: %s ',message['type'])

      logger.info('%s FileManager exiting',self.prelog)

   @staticmethod
   def md5sum(scope,filename):
      cmd = 'echo -n "' + scope + ':' + filename + '" | md5sum'
      p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
      p.wait()
      if p.returncode != 0:
         raise Exception('md5sum returned non-zero code: '+ str(p.returncode))
      stdout,stderr = p.communicate()

      return stdout.split()[0]

   @staticmethod
   def get_file_path(scope,path,filename):

      # get md5sum
      md5sum = self.md5sum(scope,filename)

      filepath = ''
      if path is not None:
         filepath = path

      # /filepath/group/phys/gener or /filepath/mc16_13TeV
      scope_parts = scope.split('.')
      for part in scope_parts:
         filepath = os.path.join(filepath,part)

      filepath = os.path.join(filepath,
                              str(md5sum[0:2]),
                              str(md5sum[2:4])
                             )
      return filepath

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
         'type':MessageTypes.OUTPUT_FILE,
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


