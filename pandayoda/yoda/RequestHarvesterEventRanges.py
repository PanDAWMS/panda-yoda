import logging,os,sys,importlib
logger = logging.getLogger(__name__)
from pandayoda.common import StatefulService,VariableWithLock,exceptions

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]

class RequestHarvesterEventRanges(StatefulService.StatefulService):
   ''' This thread is spawned to request event ranges from Harvester '''

   IDLE                    = 'IDLE'
   REQUEST                 = 'REQUEST'
   REQUESTING              = 'REQUESTING'
   NEW_EVENT_RANGES_READY  = 'NEW_EVENT_RANGES_READY'
   EXITED                  = 'EXITED'

   STATES = [IDLE,REQUEST,REQUESTING,NEW_EVENT_RANGES_READY,EXITED]

   def __init__(self,config,loop_timeout=30):
      super(RequestHarvesterEventRanges,self).__init__(loop_timeout)

      # local config options
      self.config             = config

      # set current state of the thread to IDLE
      self.state                       = VariableWithLock.VariableWithLock(self.IDLE)

      # if in state REQUEST_COMPLETE, this variable holds the event ranges retrieved
      self.new_eventranges             = VariableWithLock.VariableWithLock()

      # set if there are no more event ranges coming from Harvester
      self.no_more_eventranges_flag    = VariableWithLock.VariableWithLock(False)

      # set this to define which job definition will be used to request event ranges
      self.job_def                     = VariableWithLock.VariableWithLock()


   def reset(self):
      self.set_state(self.IDLE)
   def idle(self):
      return self.in_state(self.IDLE)
   def exited(self):
      return self.in_state(self.EXITED)

   def start_request(self,job_def):
      self.job_def.set(job_def)
      self.set_state(self.REQUEST)

   def get_eventranges(self):
      ''' parent thread calls this function to retrieve the event ranges sent by Harevester '''
      eventranges = self.new_eventranges.get()
      self.new_eventranges.set(None)
      self.set_state(self.IDLE)
      return eventranges
   def set_eventranges(self,eventranges):
      self.new_eventranges.set(eventranges)

   def new_eventranges_ready(self):
      return self.in_state(self.NEW_EVENT_RANGES_READY)

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

      while not self.exit.wait(timeout=self.loop_timeout):
         # get state
         state = self.get_state()
         logger.debug('%s start loop, current state: %s',self.prelog,state)
         
         ########
         # REQUEST State
         ########################
         if state == self.REQUEST:
            logger.debug('%s making request for event ranges',self.prelog)
            # request event ranges from Harvester
            try:
               # use messenger to request event ranges from Harvester
               messenger.request_eventranges(self.job_def.get())
            except exceptions.MessengerEventRangesAlreadyRequested:
               logger.warning('event ranges already requesting')
            
            # request events
            self.set_state(self.REQUESTING)

         
         #########
         # REQUESTING State
         ########################
         elif state == self.REQUESTING:
            logger.debug('%s checking for event ranges',self.prelog)
            # use messenger to check if event ranges are ready
            if messenger.eventranges_ready():
               logger.debug('%s event ranges are ready',self.prelog)
               # use messenger to get event ranges from Harvester
               eventranges = messenger.get_eventranges()

               # set event ranges for parent and change state
               if len(eventranges) > 0:
                  logger.debug('%s setting NEW_EVENT_RANGES variable with %d event ranges',self.prelog,len(eventranges))
                  self.set_eventranges(eventranges)
                  self.set_state(self.NEW_EVENT_RANGES_READY)
               else:
                  logger.debug('%s received no eventranges',self.prelog)
            else:
               logger.debug('%s no event ranges yet received.',self.prelog)

         else:
            logger.debug('%s nothing to do',self.prelog)

      self.set_state(self.EXITED)
      logger.debug('%s GetEventRanges thread is exiting',self.prelog)
   
