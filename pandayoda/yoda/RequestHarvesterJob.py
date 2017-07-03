import logging,os,sys,importlib
logger = logging.getLogger(__name__)
from pandayoda.common import StatefulService,VariableWithLock

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]

class RequestHarvesterJob(StatefulService.StatefulService):
   ''' This thread is spawned to request jobs from Harvester '''

   IDLE                 = 'IDLE'
   REQUEST              = 'REQUEST'
   REQUESTING           = 'REQUESTING'
   NEW_JOBS_READY       = 'NEW_JOBS_READY'
   EXITED               = 'EXITED'

   STATES = [IDLE,REQUEST,REQUESTING,NEW_JOBS_READY,EXITED]

   def __init__(self,config,loop_timeout=30):
      super(RequestHarvesterJob,self).__init__(loop_timeout)

      # local config options
      self.config             = config

      # set current state of the thread to IDLE
      self.state              = VariableWithLock.VariableWithLock(self.IDLE)

      # if in state REQUEST_COMPLETE, this variable holds the job retrieved
      self.new_jobs           = VariableWithLock.VariableWithLock()

      # set if there are no more jobs coming from Harvester
      self.no_more_jobs_flag  = VariableWithLock.VariableWithLock(False)

      # the prelog is just a string to attach before each log message
      self.prelog             = '%s:' % self.__class__.__name__

   def get_jobs(self):
      ''' parent thread calls this function to retrieve the jobs sent by Harevester '''
      jobs = self.new_jobs.get()
      self.new_jobs.set(None)
      self.set_state(self.IDLE)
      return jobs
   def set_jobs(self,job_descriptions):
      self.new_jobs.set(job_descriptions)

   def new_jobs_ready(self):
      return self.in_state(self.NEW_JOBS_READY)

   def idle(self):
      return self.in_state(self.IDLE)

   def start_request(self):
      self.set_state(self.REQUEST)

   def reset(self):
      self.set_state(self.IDLE)

   def no_more_jobs(self):
      return self.no_more_jobs_flag.get()

   def get_messenger(self):
      # get the name of the plugin from the config file
      if self.config.has_option(config_section,'messenger_plugin_module'):
         messenger_plugin_module = self.config.get(config_section,'messenger_plugin_module')
      else:
         raise Exception('%s Failed to retrieve messenger_plugin_module from config file section %s' % (self.prelog,config_section))


      # try to import the module specified in the config
      # if it is not in the PYTHONPATH this will fail
      try:
         return importlib.import_module(messenger_plugin_module)
      except ImportError:
         logger.exception('%s Failed to import messenger_plugin: %s',self.prelog,messenger_plugin_module)
         raise


   def run(self):
      ''' overriding base class function '''

      # get the messenger for communicating with Harvester
      messenger = self.get_messenger()
      messenger.setup(self.config)


      while not self.exit.wait(timeout=self.loop_timeout):
         logger.debug('%s start loop',self.prelog)
         state = self.get_state()
         logger.debug('%s current state: %s',self.prelog,state)
         if state == self.REQUEST:
            # request events
            self.set_state(self.REQUESTING)

            # get panda jobs from Harvester
            try:
               pandajobs = messenger.get_pandajobs()
            except exceptions.MessengerJobAlreadyRequested:
               logger.warning('%s tried requesting job twice',self.prelog)
            else:
               if len(pandajobs) == 0:
                  logger.debug('%s setting NO_MORE_JOBS flag and exiting',self.prelog)
                  self.no_more_jobs_flag.set(True)
                  self.stop()
               else:
                  logger.debug('%s setting NEW_JOBS variable',self.prelog)
                  self.set_jobs(pandajobs)
                  self.set_state(self.NEW_JOBS_READY)
         

      self.set_state(self.EXITED)
      logger.debug('%s GetJob thread is exiting',self.prelog)
