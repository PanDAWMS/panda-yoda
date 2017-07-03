import logging,os,sys,importlib
logger = logging.getLogger(__name__)
from pandayoda.common import StatefulService,VariableWithLock

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

      # the prelog is just a string to attach before each log message
      self.prelog                      = '%s:' % self.__class__.__name__

   def reset(self):
      self.set_state(self.IDLE)
   def idle(self):
      return self.in_state(self.IDLE)
   def exited(self):
      return self.in_state(self.EXITED)
   
   def start_request(self):
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

            # get event ranges from Harvester
            try:
               eventranges = messenger.get_eventranges()
            except exceptions.MessengerJobAlreadyRequested:
               logger.warning('%s tried requesting event ranges twice',self.prelog)
            else:
               if len(eventranges) == 0:
                  logger.debug('%s setting NO_MORE_EVENT_RANGES flag',self.prelog)
                  self.no_more_eventranges_flag.set(True)
                  self.stop()
               else:
                  logger.debug('%s setting NEW_EVENT_RANGES variable with %d event ranges',self.prelog,len(eventranges))
                  self.set_eventranges(eventranges)
                  self.set_state(self.NEW_EVENT_RANGES_READY)

      self.set_state(self.EXITED)
      logger.debug('%s GetEventRanges thread is exiting',self.prelog)
   
   def get_new_eventranges(self,block=False,timeout=None):
      ''' this function retrieves a message from the queue and returns the
          the eventranges. This should be called by the calling function, not by
          the thread.'''
      try:
         msg = self.queue.get(block=block,timeout=timeout)
      except SerialQueue.Empty:
         logger.debug('%s GetEventRanges queue is empty',self.prelog)
         return {}

      if msg['type'] ==  MessageTypes.NEW_EVENT_RANGES:
         logger.debug('%s received new event range message',self.prelog)
         return msg['eventranges']
      elif msg['type'] == MessageTypes.NO_MORE_EVENT_RANGES:
         logger.debug('%s receive no more event ranges message',self.prelog)
      else:
         logger.error('%s message type from event range request unrecognized: %s',self.prelog,msg['type'])
      return {}
