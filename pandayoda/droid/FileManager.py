import os,sys,threading,logging,shutil,subprocess
from pandayoda.common import MessageTypes,SerialQueue
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

      # this is used to trigger the thread exit
      self.exit = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the JobManager thread to exit'''
      self.exit.set()


   def run(self):
      ''' this is the function run as a subthread when the user runs fileManager_instance.start() '''
      
      # get loop_timeout
      if self.config.has_option(config_section,'loop_timeout'):
         loop_timeout = self.config.getfloat(config_section,'loop_timeout')
      else:
         logger.error('must specify "loop_timeout" in "%s" section of config file',config_section)
         return
      logger.info('%s loop_timeout: %d',config_section,loop_timeout)


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
                  
                  logger.debug('copying %s to %s',message['filename'],self.yoda_working_path)
                  shutil.copy(message['filename'],self.yoda_working_path)
               elif not os.path.exists(message['filename']):
                  logger.error('input filename does not exist: %s',message['filename'])
               elif not os.path.exists(self.yoda_working_path):
                  logger.error('output file path does not exist: %s',self.yoda_working_path)
            elif message['type'] == MessageTypes.WALLCLOCK_EXPIRING:
               logger.debug('received wallclock expiring message')
               self.stop()
               break
            else:
               logger.error('%s message type is incorrect: %s ',message['type'])

      logger.info('FileManager exiting')

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
      md5sum = FileManager.md5sum(scope,filename)

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

