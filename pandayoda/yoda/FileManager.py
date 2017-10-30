import os,sys,threading,logging,shutil,importlib
from pandayoda.common import MessageTypes,serializer,SerialQueue
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


   def get_harvester_messenger(self):
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

      # read configuration info from file
      self.read_config()

      # get the harvester_messenger for communicating with Harvester
      harvester_messenger = self.get_harvester_messenger()
      harvester_messenger.setup(self.config)



      while not self.exit.isSet():
         logger.debug('starting loop')

         # process incoming messages
         try:
            qmsg = self.queues['FileManager'].get(timeout=self.loop_timeout)
         except SerialQueue.Empty:
            logger.debug('queue is empty')
         else:

            logger.debug('message received: %s',qmsg)

            if qmsg['type'] == MessageTypes.OUTPUT_FILE:
               
               # copy file to yoda working path
               source_file = qmsg['filename']
               destination_path = self.yoda_working_path

               if os.path.exists(source_file) and os.path.exists(destination_path):
                  
                  logger.debug('copying %s to %s',source_file,destination_path)
                  shutil.copy(source_file,destination_path)
               elif not os.path.exists(source_file):
                  logger.error('input filename does not exist: %s',source_file)
               elif not os.path.exists(self.yoda_working_path):
                  logger.error('output file path does not exist: %s',destination_path)

               destination_file = os.path.join(destination_path,os.path.basename(qmsg['filename']))
               
               # add file to Harvester stage out
               harvester_messenger.stage_out_file(self.output_file_type,
                                        destination_file,
                                        qmsg['eventrangeid'],
                                        qmsg['eventstatus'],
                                        qmsg['pandaid']
                                       )


            else:
               logger.error('message type not recognized')


      # exit
      logger.info('FileManager exiting')

   def read_config(self):
      # get self.loop_timeout
      if self.config.has_option(config_section,'loop_timeout'):
         self.loop_timeout = self.config.getfloat(config_section,'loop_timeout')
      else:
         logger.error('must specify "loop_timeout" in "%s" section of config file',config_section)
         return
      logger.info('%s loop_timeout: %d',config_section,self.loop_timeout)

      # get self.output_file_type
      if self.config.has_option(config_section,'output_file_type'):
         self.output_file_type = self.config.get(config_section,'output_file_type')
      else:
         logger.error('must specify "output_file_type" in "%s" section of config file',config_section)
         return
      logger.info('%s output_file_type: %s',config_section,self.output_file_type)

      # get self.yoda_working_path
      if self.config.has_option(config_section,'yoda_working_path'):
         self.yoda_working_path = self.config.get(config_section,'yoda_working_path')
      else:
         logger.error('typically "yoda_working_path" is set by yoda_droid in the "%s" section of config',config_section)
         return
      logger.info('%s yoda_working_path: %s',config_section,self.yoda_working_path)





