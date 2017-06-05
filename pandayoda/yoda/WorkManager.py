import os,sys,threading,logging,importlib,time
from mpi4py import MPI
from pandayoda.common import MessageTypes,SerialQueue,EventRangeList
from pandayoda.common import yoda_droid_messenger as ydm
logger = logging.getLogger(__name__)

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]

class WorkManager(threading.Thread):
   ''' Work Manager: this thread manages work going to the running Droids '''

   def __init__(self,config,queues):
      ''' 
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send 
                     messages to other Droid components about errors, etc.
        config: the ConfigParser handle for yoda
        '''
      # call base class init function
      super(WorkManager,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # configuration of Yoda
      self.config                = config

      # this is used to trigger the thread exit
      self.exit                  = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the JobManager thread to exit'''
      self.exit.set()


   def run(self):
      ''' this function is executed as the subthread. '''

      # get loop_timeout
      if self.config.has_option(config_section,'loop_timeout'):
         loop_timeout = self.config.getfloat(config_section,'loop_timeout')
      else:
         logger.error('must specify "loop_timeout" in "%s" section of config file',config_section)
         return
      logger.info('loop_timeout: %d',loop_timeout)

      # this is a place holder for the GetJobRequest object
      job_request                = None
      # this is a place holder for the GetEventRangesRequest object
      eventrange_request         = None
      # list of all jobs received from Harvester key-ed by panda id
      pandajobs                  = {}
      # list of all event ranges key-ed by panda id
      eventranges                = {}
      # flag to indicate the need for a job request
      need_job_request           = True
      # flag to indicate the need for event ranges
      need_event_ranges          = True
      # flag to indicate there are no more jobs
      no_more_jobs               = False
      # flag to indicate there are no more event ranges
      no_more_event_ranges       = False
      # this is a place holder for the droid message request
      droid_msg_request          = None

      # list of droid ranks waiting for job description, contains only the source_rank
      droids_waiting_for_job     = []
      # list of droid ranks waiting for event ranges, contains only the source_rank
      droids_waiting_for_event_ranges = []


      while not self.exit.isSet():
         logger.debug('start loop')
         logger.debug('cwd: %s',os.getcwd())

         # if we need a job request, request one
         if need_job_request and not no_more_jobs:
            logger.debug('checking for new job')
            
            # if job_request has not been created, create it
            if job_request is None:
               logger.debug('requesting job from Harvester')
               job_request = GetJob(self.config)
               job_request.start()
            
            # if job request has completed, check output via queue
            if not job_request.isAlive():
               logger.debug('job request has completed')
               try:
                  msg = job_request.queue.get(block=False)
               except SerialQueue.Empty:
                  logger.error('job request queue is empty')
               else:
                  logger.debug('received message from get job request: %s',msg)
                  if msg['type'] == MessageTypes.NEW_JOB:
                     logger.debug('received new job message')
                     for jobid,job in msg['jobs'].iteritems():
                        pandajobs[jobid] = job
                     need_job_request = False
                  elif msg['type'] == MessageTypes.NO_MORE_JOBS:
                     logger.debug('receive no more jobs message')
                     no_more_jobs = True
                  else:
                     logger.error('message type from job request unrecognized.')
               # reset request
               job_request = None
            else:
               logger.debug('get job request is still running, will check next loop')
         else:
            logger.debug('do not need to request job')

         # if we need an event range request, request one
         if need_event_ranges and not no_more_event_ranges:
            logger.debug('checking for event range')

            # if job_request has not been created, create it
            if eventrange_request is None:
               logger.debug('requesting event ranges from Harvester')
               eventrange_request = GetEventRanges(self.config)
               eventrange_request.start()
            
            # if job request has completed, check output via queue
            if not eventrange_request.isAlive():
               logger.debug('event range request has completed')
               try:
                  msg = eventrange_request.queue.get(block=False)
               except SerialQueue.Empty:
                  logger.error('event range request queue is empty')
               else:
                  logger.debug('received message from get event range request: %s',msg)
                  if msg['type'] == MessageTypes.NEW_EVENT_RANGES:
                     logger.debug('received new event range')
                     for jobid,list_of_eventranges in msg['eventranges'].iteritems():
                        if jobid in eventranges:
                           eventranges[jobid] += EventRangeList.EventRangeList(list_of_eventranges)
                        else:
                           eventranges[jobid] = EventRangeList.EventRangeList(list_of_eventranges)
                     

                     need_event_ranges = False
                  elif msg['type'] == MessageTypes.NO_MORE_EVENT_RANGES:
                     logger.debug('receive no more event ranges message')
                     no_more_event_ranges = True
                  else:
                     logger.error('message type from event range request unrecognized.')
                  # reset request
                  eventrange_request = None
            else:
               logger.debug('get event range request is still running, will check next loop')
         else:
            logger.debug('do not need to request event ranges')


         # if there is work to hand out, check for messages from droid ranks
         if len(pandajobs) > 0 and len(eventranges) > 0:
            logger.debug('checking for droid messages')
            # check for in coming message from droids
            if droid_msg_request is None:
               logger.debug('requesting new message from droid')
               droid_msg_request = ydm.get_droid_message_for_workmanager()

            if droid_msg_request:
               logger.debug('checking request for message from droid')
               # test for a message
               status = MPI.Status()
               msg_received,droid_msg = droid_msg_request.test(status=status)
               if msg_received:
                  source_rank = status.Get_source()
                  logger.debug('received message from droid rank %d: %s',source_rank,droid_msg)

                  if droid_msg['type'] == MessageTypes.REQUEST_JOB:
                     logger.debug('received request for new job')

                     # check to see what events I have left to do and to which job they belong
                     # then decide which job description to send the droid rank

                     # if there is only one job, send it
                     if len(pandajobs) == 1:
                        jobid = pandajobs.keys()[0]
                     # if there are no jobs, add this rank to the queue of jobs waiting for a new job
                     elif len(pandajobs) == 0:
                        droids_waiting_for_job.append(source_rank)
                     # if there is more than one panda job, decide which job to send
                     else:
                        jobid = self.get_jobid_with_minimum_ready(eventranges)

                     # send job to Droid
                     logger.debug('sending new job to droid rank %d',source_rank)
                     ydm.send_droid_new_job(pandajobs[jobid],source_rank)
                     


                  elif droid_msg['type'] == MessageTypes.REQUEST_EVENT_RANGES:
                     logger.debug('received request for new event ranges')

                     # check the job definition which is already running on this droid rank
                     # see if there are more events to be dolled out

                     # the droid sent the current running panda id, determine if there are events left for this panda job
                     droid_jobid = droid_msg['PandaID']
                     if eventranges[droid_jobid].number_ready() > 0:
                        local_eventranges = eventranges[droid_jobid].get_next()

                        # send event ranges to Droid
                        logger.debug('sending new event range to droid rank %d',source_rank)
                        ydm.send_droid_new_eventranges(local_eventranges,source_rank)

                  else:
                     logger.error('Failed to parse message from droid rank %d: %s',source_rank,droid_msg)

                  # reset message request
                  droid_msg_request = None
               else:
                  logger.debug('waiting for message from droid')
            else:
               logger.debug('no messages from droid')
         else:
            logger.debug('no work to do so no need to get Droid messages')
         time.sleep(loop_timeout)
      logger.info('WorkManager is exiting')


   def get_jobid_with_minimum_ready(eventranges):
      
      # loop over event ranges, count the number of ready events
      job_id = 0
      job_nready = 999999
      for pandaid,erl in eventranges.iteritems():
         nready = erl.number_ready()
         if nready > 0 and nready < job_nready:
            job_id = pandaid
            job_nready = nready

      return job_id







class HarvesterRequest(threading.Thread):
   ''' This thread is spawned to request something from Harvester '''
   def __init__(self,config):
      super(HarvesterRequest,self).__init__()

      # here is a queue to return messages to the calling function
      self.queue  = SerialQueue.SerialQueue()

      # keep the configuration info
      self.config = config

      # here is a way to stop the thread
      self.exit   = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the WorkManager thread to exit'''
      self.exit.set()

   def run(self):
      ''' this function is executed as the subthread. '''
      logger.error('need to impliment this function in derived class')


   def get_messenger(self):
      # get the name of the plugin from the config file
      if self.config.has_option(config_section,'messenger_plugin_module'):
         messenger_plugin_module = self.config.get(config_section,'messenger_plugin_module')
      else:
         raise Exception('Failed to retrieve messenger_plugin_module from config file section ' + config_section)


      # try to import the module specified in the config
      # if it is not in the PYTHONPATH this will fail
      try:
         return importlib.import_module(messenger_plugin_module)
      except ImportError:
         logger.exception('Failed to import messenger_plugin: %s',messenger_plugin_module)
         raise



class GetJob(HarvesterRequest):
   ''' This is a request thread to get a jobs from Harvester '''
   def run(self):
      ''' overriding base class function '''

      # get the messenger for communicating with Harvester
      messenger = self.get_messenger()
      messenger.setup(self.config)

      # get panda jobs from Harvester
      pandajobs = messenger.get_pandajobs()

      if len(pandajobs) == 0:
         self.queue.put({'type':MessageTypes.NO_MORE_JOBS})
      else:
         self.queue.put({'type':MessageTypes.NEW_JOB,'jobs':pandajobs})



class GetEventRanges(HarvesterRequest):
   ''' This is a request thread to get a event ranges from Harvester '''
   def run(self):
      ''' overriding base class function '''

      # get the messenger for communicating with Harvester
      messenger = self.get_messenger()
      messenger.setup(self.config)

      # get panda jobs from Harvester
      eventranges = messenger.get_eventranges()

      if len(eventranges) == 0:
         self.queue.put({'type':MessageTypes.NO_MORE_EVENT_RANGES})
      else:
         self.queue.put({'type':MessageTypes.NEW_EVENT_RANGES,'eventranges':eventranges})









