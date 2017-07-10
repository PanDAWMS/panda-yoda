import os,sys,threading,logging,shutil,importlib
from mpi4py import MPI
from pandayoda.common import MessageTypes,serializer
from pandayoda.common import yoda_droid_messenger as ydm
from pandayoda.yoda import DroidRequestList
logger = logging.getLogger(__name__)

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


class FileManager(threading.Thread):


   IDLE                 = 'IDLE'
   STAGE_OUT            = 'STAGE_OUT'
   STAGING_OUT          = 'STAGING_OUT'
   EXITED               = 'EXITED'

   STATES = [IDLE,STAGE_OUT,STAGING_OUT,EXITED]

   def __init__(self,config,queues):
      super(FileManager,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # configuration of Yoda
      self.config                = config

      # this is used to trigger the thread exit
      self.exit                  = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the JobManager thread to exit'''
      self.exit.set()


   def get_messenger(self):
      # get the name of the plugin from the config file
      if self.config.has_option(config_section,'messenger_plugin_module'):
         messenger_plugin_module = self.config.get(config_section,'messenger_plugin_module')
      else:
         raise Exception('Failed to retrieve messenger_plugin_module from config file section %s' % config_section)


      # try to import the module specified in the config
      # if it is not in the PYTHONPATH this will fail
      try:
         return importlib.import_module(messenger_plugin_module)
      except ImportError:
         logger.exception('Failed to import messenger_plugin: %s',messenger_plugin_module)
         raise



   def run(self):
      logger.debug('starting thread')

      # get loop_timeout
      if self.config.has_option(config_section,'loop_timeout'):
         loop_timeout = self.config.getfloat(config_section,'loop_timeout')
      else:
         logger.error('must specify "loop_timeout" in "%s" section of config file',config_section)
         return
      logger.info('%s loop_timeout: %d',config_section,loop_timeout)

      # get output_file_type
      if self.config.has_option(config_section,'output_file_type'):
         output_file_type = self.config.get(config_section,'output_file_type')
      else:
         logger.error('must specify "output_file_type" in "%s" section of config file',config_section)
         return
      logger.info('%s output_file_type: %s',config_section,output_file_type)

      # get yoda_working_path
      if self.config.has_option(config_section,'yoda_working_path'):
         yoda_working_path = self.config.get(config_section,'yoda_working_path')
      else:
         logger.error('typically "yoda_working_path" is set by yoda_droid in the "%s" section of config',config_section)
         return
      logger.info('%s yoda_working_path: %s',config_section,yoda_working_path)


      # there can be multiple droid requests running at any given time.
      # this list keeps track of them
      droid_requests = DroidRequestList.DroidRequestList(self.config,ydm.TO_YODA_FILEMANAGER,loop_timeout,self.__class__.__name__)


      # get the messenger for communicating with Harvester
      messenger = self.get_messenger()
      messenger.setup(self.config)



      while not self.exit.wait(timeout=loop_timeout):
         logger.debug('starting loop')

         # if no droid requests are waiting for droid messages, create a new one
         if droid_requests.number_waiting_for_droid_message() <= 0:
            logger.debug('adding request')
            droid_requests.add_request()

         # process the droid requests that are waiting for a response
         for request in droid_requests.get_waiting():
            # get the message from droid
            msg = request.droid_msg.get()

            logger.debug('message received from droid rank %s: %s',request.get_droid_rank(),msg)

            if 'output_file_data' in msg:
               # extract the file data from the message
               output_file_data = msg['output_file_data']

               # copy file to yoda working path
               source_file = output_file_data['filename']
               destination_path = yoda_working_path

               if os.path.exists(source_file) and os.path.exists(destination_path):
                  
                  logger.debug('copying %s to %s',source_file,destination_path)
                  shutil.copy(source_file,destination_path)
               elif not os.path.exists(source_file):
                  logger.error('input filename does not exist: %s',source_file)
               elif not os.path.exists(yoda_working_path):
                  logger.error('output file path does not exist: %s',destination_path)

               destination_file = os.path.join(destination_path,os.path.basename(output_file_data['filename']))
               
               # add file to Harvester stage out
               messenger.stage_out_file(output_file_type,
                                        destination_file,
                                        output_file_data['eventrangeid'],
                                        output_file_data['eventstatus'],
                                        output_file_data['pandaid']
                                       )

               # tell request to exit because it is done
               request.stop()

            else:
               logger.error('droid message had no output file data inside.')







