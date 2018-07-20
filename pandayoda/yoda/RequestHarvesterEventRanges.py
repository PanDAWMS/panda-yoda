import logging,os,time,multiprocessing
from pandayoda.common import StatefulService,exceptions
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

   def __init__(self,config,job_def,mpmgr,harvester_messenger):
      super(RequestHarvesterEventRanges,self).__init__()

      # local config options
      self.config                      = config

      # these variables used to return new event ranges
      self.mgr                         = mpmgr
      self.new_eventranges             = self.mgr.dict()
      self.eventranges_avail           = multiprocessing.Event()

      # set if there are no more event ranges coming from Harvester
      self.no_more_eventranges_flag    = multiprocessing.Event()

      # set this to define which job definition will be used to request event ranges
      self.job_def                     = job_def

      # messenger module for communicating with harvester
      self.harvester_messenger         = harvester_messenger

      self.set_state(self.CREATED)


   def exited(self):
      return self.in_state(self.EXITED)

   def running(self):
      if self.get_state() in self.RUNNING_STATES:
         return True
      return False

   def get_eventranges(self):
      ''' parent thread calls this function to retrieve the event ranges sent by Harevester '''
      return self.new_eventranges

   def set_eventranges(self,eventranges):
      for key in eventranges.keys():
         self.new_eventranges[key] = eventranges[key]
         self.eventranges_avail.set()
   
   def eventranges_ready(self):
      return self.eventranges_avail.is_set()

   def no_more_eventranges(self):
      return self.no_more_eventranges_flag.is_set()

   def run(self):
      ''' overriding base class function '''

      # get the messenger for communicating with Harvester
      logger.debug('starting requestHarvesterEventRanges thread')
      
      if config_section in self.config:
         # read log level:
         if 'loglevel' in self.config[config_section]:
            self.loglevel = self.config[config_section]['loglevel']
            logger.info('%s loglevel: %s',config_section,self.loglevel)
            logger.setLevel(logging.getLevelName(self.loglevel))
         else:
            logger.warning('no "loglevel" in "%s" section of config file, keeping default',config_section)

         # read droid loop timeout:
         if 'loop_timeout' in self.config[config_section]:
            self.loop_timeout = int(self.config[config_section]['loop_timeout'])
            logger.info('%s loop_timeout: %s',config_section,self.loop_timeout)
         else:
            logger.warning('no "loop_timeout" in "%s" section of config file, keeping default %s',config_section,self.loop_timeout)

         # read droid eventrange_timeout:
         if 'eventrange_timeout' in self.config[config_section]:
            self.eventrange_timeout = int(self.config[config_section]['eventrange_timeout'])
            logger.info('%s eventrange_timeout: %s',config_section,self.eventrange_timeout)
         else:
            self.eventrange_timeout = 0
            logger.warning('no "eventrange_timeout" in "%s" section of config file, keeping default %s',config_section,self.eventrange_timeout)

      else:
         raise Exception('no %s section in the configuration' % config_section)
      
      
      # start in the request state
      self.set_state(self.REQUEST)

      while not self.exit.is_set():
         # get state
         logger.debug('start loop, current state: %s',self.get_state())
         
         ########
         # REQUEST State
         ########################
         if self.get_state() == self.REQUEST:
            logger.debug('making request for event ranges')

            # first check if events already on disk
            if self.harvester_messenger.eventranges_ready():
               logger.debug('event ranges already ready, skipping request')
               self.set_state(self.RETRIEVE_EVENTS)

            else:
               # request event ranges from Harvester
               try:
                  # use messenger to request event ranges from Harvester
                  self.harvester_messenger.request_eventranges(self.job_def)
                  request_time = time.time()
               except exceptions.MessengerEventRangesAlreadyRequested:
                  logger.warning('event ranges already requesting')
               
               # request events
               self.set_state(self.WAITING)

         
         #########
         # WAITING State
         ########################
         if self.get_state() == self.WAITING:
            logger.debug('checking for event ranges, will block for %s',self.loop_timeout)
            # use messenger to check if event ranges are ready
            if self.harvester_messenger.eventranges_ready(block=True,timeout=self.loop_timeout):
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
               eventranges = self.harvester_messenger.get_eventranges()

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
   
