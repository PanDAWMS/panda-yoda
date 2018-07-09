import os,threading,logging,importlib,time
from pandayoda.common import MessageTypes,SerialQueue
logger = logging.getLogger(__name__)

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


class FileManager(threading.Thread):


   IDLE                 = 'IDLE'
   STAGE_OUT            = 'STAGE_OUT'
   STAGING_OUT          = 'STAGING_OUT'
   EXITED               = 'EXITED'

   STATES = [IDLE,STAGE_OUT,STAGING_OUT,EXITED]

   def __init__(self,config,queues,yoda_working_path):
      super(FileManager,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # configuration of Yoda
      self.config                = config

      # this is used to trigger the thread exit
      self.exit                  = threading.Event()

      # this is the working directory of yoda
      self.yoda_working_path     = yoda_working_path

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

      local_filelist = []

      last_check = time.time()

      while not self.exit.isSet():
         logger.debug('starting loop, local_filelist size: %s',len(local_filelist))

         # process incoming messages
         try:
            qmsg = self.queues['FileManager'].get(timeout=self.loop_timeout)
         except SerialQueue.Empty:
            logger.debug('queue is empty')
         else:

            logger.debug('message received: %s',qmsg)

            if qmsg['type'] == MessageTypes.OUTPUT_FILE:

               local_filelist += qmsg['filelist']

               # I don't want to constantly check to see if the output file exists
               # so I'll only check every 10 seconds
               if time.time() - last_check > 10:
                  last_check = time.time()

                  # if an output file already exists,
                  # wait for harvester to read in file, so add file list to
                  # local running list
                  if not harvester_messenger.stage_out_file_exists():
                     # add file to Harvester stage out
                     harvester_messenger.stage_out_files(
                                                      local_filelist,
                                                      self.output_file_type
                                                     )
                     local_filelist = []
            else:
               logger.error('message type not recognized')


      # exit
      logger.info('FileManager exiting')

   def read_config(self):

      # read log level:
      if self.config.has_option(config_section,'loglevel'):
         self.loglevel = self.config.get(config_section,'loglevel')
         logger.info('%s loglevel: %s',config_section,self.loglevel)
         logger.setLevel(logging.getLevelName(self.loglevel))
      else:
         logger.warning('no "loglevel" in "%s" section of config file, keeping default',config_section)

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






