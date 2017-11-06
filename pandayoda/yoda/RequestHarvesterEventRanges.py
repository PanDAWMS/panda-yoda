import logging,os,sys,importlib
logger = logging.getLogger(__name__)
from pandayoda.common import StatefulService,VariableWithLock,exceptions

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]

class RequestHarvesterEventRanges(StatefulService.StatefulService):
   ''' This thread is spawned to request event ranges from Harvester '''

   CREATED                 = 'CREATED'
   REQUEST                 = 'REQUEST'
   WAITING                 = 'WAITING'
   EXITED                  = 'EXITED'

   STATES = [CREATED,REQUEST,WAITING,EXITED]
   RUNNING_STATES = [REQUEST,WAITING]

   def __init__(self,config,job_def):
      super(RequestHarvesterEventRanges,self).__init__()

      # local config options
      self.config                      = config

      # set current state of the thread to CREATED
      self.state                       = VariableWithLock.VariableWithLock(self.CREATED)

      # if in state REQUEST_COMPLETE, this variable holds the event ranges retrieved
      self.new_eventranges             = VariableWithLock.VariableWithLock()

      # set if there are no more event ranges coming from Harvester
      self.no_more_eventranges_flag    = VariableWithLock.VariableWithLock(False)

      # set this to define which job definition will be used to request event ranges
      self.job_def                     = job_def


   def exited(self):
      return self.in_state(self.EXITED)

   def running(self):
      if self.get_state() in self.RUNNING_STATES:
         return True
      return False

   def get_eventranges(self):
      ''' parent thread calls this function to retrieve the event ranges sent by Harevester '''
      return self.new_eventranges.get()
   def set_eventranges(self,eventranges):
      self.new_eventranges.set(eventranges)
   def eventranges_ready(self):
      if self.new_eventranges.get() is not None:
         return True
      return False

   def no_more_eventranges(self):
      return self.no_more_eventranges_flag.get()


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
      ''' overriding base class function '''

      # get the messenger for communicating with Harvester
      messenger = self.get_messenger()
      messenger.setup(self.config)

      # read in loop_timeout
      if self.config.has_option(config_section,'loop_timeout'):
         messenger_plugin_module = self.config.get(config_section,'loop_timeout')
      

      # start in the request state
      self.set_state(self.REQUEST)

      while not self.exit.wait(timeout=self.loop_timeout):
         # get state
         logger.debug('start loop, current state: %s',self.get_state())
         
         ########
         # REQUEST State
         ########################
         if self.get_state() == self.REQUEST:
            logger.debug('making request for event ranges')
            # request event ranges from Harvester
            try:
               # use messenger to request event ranges from Harvester
               messenger.request_eventranges(self.job_def)
            except exceptions.MessengerEventRangesAlreadyRequested:
               logger.warning('event ranges already requesting')
            
            # request events
            self.set_state(self.WAITING)

         
         #########
         # REQUESTING State
         ########################
         elif self.get_state() == self.WAITING:
            logger.debug('checking for event ranges')
            # use messenger to check if event ranges are ready
            if messenger.eventranges_ready():
               logger.debug('event ranges are ready')
               # use messenger to get event ranges from Harvester
               eventranges = messenger.get_eventranges()

               # set event ranges for parent and change state
               if len(eventranges) > 0:
                  logger.debug('setting NEW_EVENT_RANGES variable with %d event ranges',len(eventranges))
                  self.set_eventranges(eventranges)
                  self.stop()
               else:
                  logger.debug('received no eventranges: %s',eventranges)
                  self.stop()
            else:
               logger.debug('no event ranges yet received.')

         else:
            logger.debug('nothing to do')

      self.set_state(self.EXITED)
      logger.debug('GetEventRanges thread is exiting')
   
