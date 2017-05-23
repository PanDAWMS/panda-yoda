import os,sys,threading,logging,shutil,importlib
from pandayoda.common import MessageTypes,SerialQueue
logger = logging.getLogger(__name__)

class HarvesterComm(threading.Thread):
   ''' Harvester Comm: this thread manages the communication with Harvester '''

   config_messenger_plugin_setting = 'messenger_plugin'

   def __init__(self,queues,config
                loopTimeout      = 30):
      ''' 
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send 
                     messages to other Droid components about errors, etc.
        config: the ConfigParser handle for yoda
        loopTimeout: A positive number of seconds to block when retrieving 
                     a message from the inputQueue.'''
      # call base class init function
      super(HarvesterComm,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # configuration of Yoda
      self.config                = config

      # the timeout duration 
      self.loopTimeout           = loopTimeout

      # this is used to trigger the thread exit
      self.exit                  = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the HarvesterComm thread to exit'''
      self.exit.set()

   def run(self):
      ''' this function is executed as the subthread. '''

      # get the messenger plugin to be used to communicate with Harvester
      messenger = self.get_messenger()

      # setup messenger
      messenger.setup(self.config)

      waiting_for_jobs = False

      while not self.exit.isSet():
         logger.debug('start loop')

         # check for message from queue
         msg = self.queues['HarvesterComm'].get(timeout=self.loopTimeout)
         except SerialQueue.Empty:
            logger.debug('no message on HarvesterComm queue')
         else:
            logger.debug('received message from HarvesterComm: %s',msg)

            if msg['type'] == MessageTypes.REQUEST_JOB:

               if not waiting_for_jobs:
                  # request a job from Harvester
                  messenger.requestjobs()
                  waiting_for_jobs = True



   def get_messenger(self):
      # get the name of the plugin from the config file
      messenger_plugin_name = self.config.get(self.__class__.__name__,self.config_messenger_plugin_setting)

      # try to import the module specified in the config
      # if it is not in the PYTHONPATH this will fail
      try:
         return importlib.import_module(messenger_plugin_name)
      except ImportError:
         logger.exception('Failed to import messenger_plugin: %s',messenger_plugin_name)
         raise
      


      
