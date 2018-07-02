import logging,os,importlib,time
from pandayoda.common import StatefulService,VariableWithLock,exceptions
logger = logging.getLogger(__name__)

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


class RequestHarvesterEventRanges(StatefulService.StatefulService):
   ''' This thread is spawned to request event ranges from Harvester '''

   CREATED                 = 'CREATED'
   REQUEST                 = 'REQUEST'
   WAITING                 = 'WAITING'
   RETRIEVE_EVENTS         = 'RETRIEVE_EVENTS'
   EXITED                  = 'EXITED'

   STATES = [CREATED,REQUEST,WAITING,RETRIEVE_EVENTS,EXITED]
   RUNNING_STATES = [CREATED,REQUEST,WAITING,RETRIEVE_EVENTS]

   def __init__(self,config,job_def):
      super(RequestHarvesterEventRanges,self).__init__()

      # local config options
      self.config                      = config

      # if in state REQUEST_COMPLETE, this variable holds the event ranges retrieved
      self.new_eventranges             = VariableWithLock.VariableWithLock()

      # set if there are no more event ranges coming from Harvester
      self.no_more_eventranges_flag    = VariableWithLock.VariableWithLock(False)

      # set this to define which job definition will be used to request event ranges
      self.job_def                     = job_def

      self.set_state(self.CREATED)


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
      logger.debug('starting requestHarvesterEventRanges thread')
      messenger = self.get_messenger()
      messenger.setup(self.config)
      logger.debug('got messenger')
      
      # read in loop_timeout
      if self.config.has_option(config_section,'loop_timeout'):
         self.loop_timeout = self.config.getint(config_section,'loop_timeout')
      logger.debug('got timeout: %s',self.loop_timeout)

      # read log level:
      if self.config.has_option(config_section,'loglevel'):
         self.loglevel = self.config.get(config_section,'loglevel')
         logger.info('%s loglevel: %s',config_section,self.loglevel)
         logger.setLevel(logging.getLevelName(self.loglevel))
      else:
         logger.warning('no "loglevel" in "%s" section of config file, keeping default',config_section)


      # read timeout for waiting for event ranges:
      if self.config.has_option(config_section,'eventrange_timeout'):
         self.eventrange_timeout = self.config.getint(config_section,'eventrange_timeout')
         logger.info('%s eventrange_timeout: %s',config_section,self.loglevel)
         logger.setLevel(logging.getLevelName(self.loglevel))
      else:
         logger.warning('no "eventrange_timeout" in "%s" section of config file, keeping default',config_section)
         self.eventrange_timeout = -1
      
      
      # start in the request state
      self.set_state(self.REQUEST)

      while not self.exit.isSet():
         # get state
         logger.debug('start loop, current state: %s',self.get_state())
         
         ########
         # REQUEST State
         ########################
         if self.get_state() == self.REQUEST:
            logger.debug('making request for event ranges')

            # first check if events already on disk
            if messenger.eventranges_ready():
               logger.debug('event ranges already ready, skipping request')
               self.set_state(self.RETRIEVE_EVENTS)

            else:
               # request event ranges from Harvester
               try:
                  # use messenger to request event ranges from Harvester
                  messenger.request_eventranges(self.job_def)
                  request_time = time.time()
               except exceptions.MessengerEventRangesAlreadyRequested:
                  logger.warning('event ranges already requesting')
               
               # request events
               self.set_state(self.WAITING)

         
         #########
         # REQUESTING State
         ########################
         if self.get_state() == self.WAITING:
            logger.debug('checking for event ranges, will block for %s',self.loop_timeout)
            # use messenger to check if event ranges are ready
            if messenger.eventranges_ready(block=True,timeout=self.loop_timeout):
               self.set_state(self.RETRIEVE_EVENTS)
            else:
               logger.debug('no event ranges yet received.')
               time_waiting = time.time() - request_time
               if time_waiting > self.eventrange_timeout:
                  logger.info('have been waiting for eventranges for %d seconds, limited to %d, triggering exit',time_waiting,self.eventrange_timeout)
                  self.no_more_eventranges_flag.set(True)
                  self.stop()
                  continue

         #########
         #  RETRIEVE_EVENTS State
         ########################
         if self.get_state() == self.RETRIEVE_EVENTS:
            logger.debug('reading event ranges')
            # use messenger to get event ranges from Harvester
            try:
               eventranges = messenger.get_eventranges()

               # set event ranges for parent and change state
               if len(eventranges) > 0:
                  if logger.getEffectiveLevel() == logging.DEBUG:
                     tmpstr = ''
                     for jobid in eventranges.keys():
                        tmpstr += ' %s:%s' % (jobid,len(eventranges[jobid]))
                     logger.debug('received new eventranges for PandaID:N-ranges: %s',tmpstr)
                     
                  self.set_eventranges(eventranges)
                  
                  # if Harvester provided no event ranges for this panda ID, then set the no more events flag
                  if len(eventranges[str(self.job_def['pandaID'])]) == 0:
                     logger.debug('no new event ranges received. setting flag')
                     self.no_more_eventranges_flag.set(True)

                  self.stop()
               else:
                  logger.debug('received no eventranges: %s',eventranges)
                  self.stop()
            except exceptions.MessengerFailedToParse,e:
               logger.error('failed to parse an event file: %s',str(e))
               self.stop()

      self.set_state(self.EXITED)
      logger.debug('thread is exiting')
   
