import os,sys,threading,logging,importlib,time
from mpi4py import MPI
from pandayoda.common import MessageTypes,SerialQueue,EventRangeList,exceptions
from pandayoda.common import yoda_droid_messenger as ydm,VariableWithLock
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


      # get send_n_eventranges
      if self.config.has_option(config_section,'send_n_eventranges'):
         send_n_eventranges = self.config.getint(config_section,'send_n_eventranges')
      else:
         logger.error('must specify "send_n_eventranges" in "%s" section of config file',config_section)
         return
      logger.info('send_n_eventranges: %d',send_n_eventranges)

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
      # flag to indicate there are no more event ranges
      no_more_event_ranges       = False
      # this is a place holder for the droid message request
      droid_msg_request          = None


      # setup subthreads
      threads = {}
      threads['GetJob']          = GetJob(self.config,loop_timeout)
      threads['GetJob'].start()

      threads['GetEventRanges']  = GetEventRanges(self.config,loop_timeout)
      threads['GetEventRanges'].start()
      
      # there can be multiple droid requests running at any given time.
      # this list keeps track of them
      droid_requests = DroidRequestList()

      while not self.exit.isSet():
         logger.debug('start loop')
         logger.debug('cwd: %s',os.getcwd())

         logger.debug('checking all threads are still running and if they have messages')
         for name,thread in threads.iteritems():
            if not thread.isAlive() or thread.get_state() == HarvesterRequest.EXITED:
               logger.error('%s has exited',name)

         ###############
         # check for a new job
         ################################
         logger.debug('checking for new job')
         if threads['GetJob'].request_complete():
            logger.debug('retrieving job from GetJob queue')
            new_jobs = threads['GetJob'].get_new_job()
            # add new_jobs to list
            for jobid,job in new_jobs.iteritems():
               # add job to list of jobs orderd by job id
               pandajobs[jobid] = job
         elif threads['GetJob'].no_more_jobs():
            logger.debug('there are no more jobs')
         else:
            logger.debug('GetJob state is %s',threads['GetJob'].get_state())



         ###############
         # check for a new event range
         ################################
         logger.debug('checking for event range')
         if threads['GetEventRanges'].request_complete():
            logger.debug('retrieving event range from GetEventRanges queue')
            new_eventranges = threads['GetEventRanges'].get_new_eventranges()

            # if no ranges were returned, there are none left so stop the GetEventRanges thread
            if len(new_eventranges) == 0: 
               threads['GetEventRanges'].stop()
            # otherwise parse the list
            else:
               # add new event ranges to the list
               for jobid,list_of_eventranges in new_eventranges.iteritems():
                  if jobid in eventranges:
                     eventranges[jobid] += EventRangeList.EventRangeList(list_of_eventranges)
                  else:
                     eventranges[jobid] = EventRangeList.EventRangeList(list_of_eventranges)
         elif threads['GetEventRanges'].no_more_event_ranges():
            logger.debug('there are no more event ranges')
         else:
            logger.debug('GetEventRanges state is %s',threads['GetEventRanges'].get_state())
            

         ################
         # check for Droid requests
         ################################
         logger.debug('checking for droid messages')

         # process the droid requests that are waiting for a response
         for request in droid_requests.get_waiting():
            # get the message
            msg = request.droid_msg.get()
            
            #############
            ## DROID requesting new job
            ###############################
            if msg['type'] == MessageTypes.REQUEST_JOB:

               # check to see what events I have left to do and to which job they belong
               # then decide which job description to send the droid rank

               # if there is only one job, send it
               if len(pandajobs) > 0:
                  if len(pandajobs) == 1:
                     jobid = pandajobs.keys()[0]
                     logger.debug('only one job so sending it')
                  else:
                     logger.warning('more than one job, not sure this will work yet')
                     jobid = self.get_jobid_with_minimum_ready(eventranges)

                  request.send_job(pandajobs[jobid])
               # if there are no jobs, add this rank to the queue of jobs waiting for a new job
               else:
                  logger.debug('no job available yet. try next loop')
            
            #############
            ## DROID requesting new event ranges
            ###############################
            elif msg['type'] == MessageTypes.REQUEST_EVENT_RANGES:

               # check the job definition which is already running on this droid rank
               # see if there are more events to be dolled out

               # send the number of event ranges equal to the number of Athena workers

               # the droid sent the current running panda id, determine if there are events left for this panda job
               droid_jobid = msg['PandaID']
               if eventranges[droid_jobid].number_ready() > 0:
                  if eventranges[droid_jobid].number_ready() >= send_n_eventranges:
                     local_eventranges = eventranges[droid_jobid].get_next(send_n_eventranges)
                  else:
                     local_eventranges = eventranges[droid_jobid].get_next(eventranges[droid_jobid].number_ready())
                  
                  # send event ranges to DroidRequest
                  logger.debug('sending %d new event ranges for droid request',len(local_eventranges))
                  request.send_eventranges(local_eventranges)
               else:
                  logger.debug('droid request asking for eventranges, but no event ranges left in local list')
                  GetEventRanges_state = threads['GetEventRanges'].get_state()
                  logger.debug('GetEventRanges thread is in the state %s',GetEventRanges_state)
                  if GetEventRanges_state == HarvesterRequest.IDLE:
                     logger.debug('Setting GetEventRanges state to REQUEST to trigger new request')
                     threads['GetEventRanges'].set_state(HarvesterRequest.REQUEST)
                  elif GetEventRanges_state == HarvesterRequest.EXITED:
                     logger.debug('GetEventRanges has exited so no more ranges to get')
                     request.send_no_more_eventranges()

            else:
               logger.error('message type was not recognized: %s',msg['type'])
         
         # if no droid requests are waiting for droid messages, create a new one
         if droid_requests.number_waiting_for_droid_message() <= 0:
            droid_requests.add_request(self.config,loop_timeout)
         

         # if there is nothing to be done, sleep
         if not threads['GetJob'].request_complete() and \
            not threads['GetEventRanges'].request_complete() and \
            droid_requests.number_running() == 0:
            time.sleep(loop_timeout)
         else:
            time.sleep(1)

      for name,thread in threads.iteritems():
         logger.info('signaling exit to thread %s',name)
         thread.stop()
      for name,thread in threads.iteritems():
         logger.info('waiting for %s to join',name)
         thread.join()
      

      logger.info('WorkManager is exiting')

   def number_eventranges_ready(self,eventranges):
      total = 0
      for id,range in eventranges.iteritems():
         total += range.number_ready()
      return total

   def get_jobid_with_minimum_ready(self,eventranges):
      
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

   IDLE                 = 'IDLE'
   REQUEST              = 'REQUEST'
   REQUESTING           = 'REQUESTING'
   REQUEST_COMPLETE     = 'REQUEST_COMPLETE'
   EXITED               = 'EXITED'

   STATES = [IDLE,REQUEST,REQUESTING,REQUEST_COMPLETE,EXITED]

   def __init__(self,config,loop_timeout=30):
      super(HarvesterRequest,self).__init__()

      # here is a queue to return messages to the calling function
      self.queue        = SerialQueue.SerialQueue()

      # keep the configuration info
      self.config       = config

      # control loop execution frequency
      self.loop_timeout = loop_timeout

      # here is a way to stop the thread
      self.exit         = threading.Event()

      # current state of the thread
      self.state        = VariableWithLock.VariableWithLock(HarvesterRequest.IDLE)

   def stop(self):
      ''' this function can be called by outside threads to cause the WorkManager thread to exit'''
      self.exit.set()

   def get_state(self):
      return self.state.get()

   def set_state(self,state):
      if state in HarvesterRequest.STATES:
         self.state.set(state)
      else:
         logger.error('HarvesterRequest: tried to set state %s which is not supported',state)

   def request_complete(self):
      if self.get_state() == HarvesterRequest.REQUEST_COMPLETE:
         return True
      return False

   def run(self):
      ''' this function is executed as the subthread. '''
      logger.error('HarvesterRequest: need to impliment this function in derived class')


   def get_messenger(self):
      # get the name of the plugin from the config file
      if self.config.has_option(config_section,'messenger_plugin_module'):
         messenger_plugin_module = self.config.get(config_section,'messenger_plugin_module')
      else:
         raise Exception('HarvesterRequest: Failed to retrieve messenger_plugin_module from config file section ' + config_section)


      # try to import the module specified in the config
      # if it is not in the PYTHONPATH this will fail
      try:
         return importlib.import_module(messenger_plugin_module)
      except ImportError:
         logger.exception('HarvesterRequest: Failed to import messenger_plugin: %s',messenger_plugin_module)
         raise

class GetJob(HarvesterRequest):
   ''' This is a request thread to get a jobs from Harvester '''
   def run(self):
      ''' overriding base class function '''

      # get the messenger for communicating with Harvester
      messenger = self.get_messenger()
      messenger.setup(self.config)

      # always request an event first thing
      self.set_state(HarvesterRequest.REQUEST)

      while not self.exit.wait(timeout=self.loop_timeout):
         logger.debug('GetJob: start loop')
         state = self.get_state()
         logger.debug('GetJob: current state: %s',state)
         if state == HarvesterRequest.IDLE:
            # nothing to do
            continue
         elif state == HarvesterRequest.REQUEST:
            # request events
            self.set_state(HarvesterRequest.REQUESTING)

            # get panda jobs from Harvester
            try:
               pandajobs = messenger.get_pandajobs()
            except exceptions.MessengerJobAlreadyRequested:
               logger.warning('GetJob: tried requesting job twice')
            else:
               if len(pandajobs) == 0:
                  logger.debug('GetJob: sending NO_MORE_JOBS message')
                  self.queue.put({'type':MessageTypes.NO_MORE_JOBS})
                  self.stop()
               else:
                  logger.debug('GetJob: sending NEW_JOB message')
                  self.queue.put({'type':MessageTypes.NEW_JOB,'jobs':pandajobs})
                  self.set_state(HarvesterRequest.REQUEST_COMPLETE)
         elif state == HarvesterRequest.REQUEST_COMPLETE:
            # return to IDLE state if the queue is now empty, meaning the job has been retrieved
            if self.queue.empty():
               self.set_state(HarvesterRequest.IDLE)

      self.set_state(HarvesterRequest.EXITED)
      logger.debug('GetJob thread is exiting')

   def get_new_job(self,block=False,timeout=None):
      ''' this function retrieves a message from the queue and returns the
          the job definitions.'''
      try:
         msg = self.queue.get(block=block,timeout=timeout)
      except SerialQueue.Empty:
         logger.debug('GetJob: GetJob queue is empty')
         return {}

      if msg['type'] == MessageTypes.NEW_JOB:
         logger.debug('GetJob: received new job message')
         return msg['jobs']
      elif msg['type'] == MessageTypes.NO_MORE_JOBS:
         logger.debug('GetJob: receive no more jobs message')
      else:
         logger.error('GetJob: message type from job request unrecognized: %s',msg['type'])
      return {}
   
   def no_more_jobs(self):
      if self.queue.empty() and self.get_state() == HarvesterRequest.EXITED:
         return True
      return False


class GetEventRanges(HarvesterRequest):
   ''' This is a request thread to get a event ranges from Harvester '''
   def run(self):
      ''' overriding base class function '''

      # get the messenger for communicating with Harvester
      messenger = self.get_messenger()
      messenger.setup(self.config)

      # always request an event first thing
      self.set_state(HarvesterRequest.REQUEST)

      while not self.exit.wait(timeout=self.loop_timeout):
         logger.debug('GetEventRanges: start loop')

         state = self.get_state()
         logger.debug('GetEventRanges: current state: %s',state)
         if state == HarvesterRequest.IDLE:
            # nothing to do
            continue
         elif state == HarvesterRequest.REQUEST:
            # request events
            self.set_state(HarvesterRequest.REQUESTING)

            # get event ranges from Harvester
            try:
               eventranges = messenger.get_eventranges()
            except exceptions.MessengerJobAlreadyRequested:
               logger.warning('GetEventRanges: tried requesting event ranges twice')
            else:
               if len(eventranges) == 0:
                  logger.debug('GetEventRanges: sending NO_MORE_EVENT_RANGES message')
                  self.queue.put({'type':MessageTypes.NO_MORE_EVENT_RANGES})
               else:
                  logger.debug('GetEventRanges: sending NEW_EVENT_RANGES message with %d jobids',len(eventranges))
                  self.queue.put({'type':MessageTypes.NEW_EVENT_RANGES,'eventranges':eventranges})
               
               self.set_state(HarvesterRequest.REQUEST_COMPLETE)
         
         elif state == HarvesterRequest.REQUEST_COMPLETE:
            # return to IDLE state if the queue is now empty, meaning the job has been retrieved
            if self.queue.empty():
               self.set_state(HarvesterRequest.IDLE)

      self.set_state(HarvesterRequest.EXITED)
      logger.debug('GetEventRanges: GetEventRanges thread is exiting')
   
   def get_new_eventranges(self,block=False,timeout=None):
      ''' this function retrieves a message from the queue and returns the
          the eventranges. This should be called by the calling function, not by
          the thread.'''
      try:
         msg = self.queue.get(block=block,timeout=timeout)
      except SerialQueue.Empty:
         logger.debug('GetEventRanges: GetEventRanges queue is empty')
         return {}

      if msg['type'] ==  MessageTypes.NEW_EVENT_RANGES:
         logger.debug('GetEventRanges: received new event range message')
         return msg['eventranges']
      elif msg['type'] == MessageTypes.NO_MORE_EVENT_RANGES:
         logger.debug('GetEventRanges: receive no more event ranges message')
      else:
         logger.error('GetEventRanges: message type from event range request unrecognized: %s',msg['type'])
      return {}

   def no_more_event_ranges(self):
      if self.queue.empty() and self.get_state() == HarvesterRequest.EXITED:
         return True
      return False
            



class DroidRequest(threading.Thread):
   ''' This thread manages one message from Droid ranks '''

   CREATED              = 'CREATED'
   REQUESTING           = 'REQUESTING'
   MESSAGE_RECEIVED     = 'MESSAGE_RECEIVED'
   RECEIVED_JOB         = 'RECEIVED_JOB'
   RECEIVED_EVENTRANGES = 'RECEIVED_EVENTRANGES'
   EXITED               = 'EXITED'

   STATES = [CREATED,REQUESTING,MESSAGE_RECEIVED,RECEIVED_JOB,RECEIVED_EVENTRANGES,EXITED]
   RUNNING_STATES = [CREATED,REQUESTING,MESSAGE_RECEIVED,RECEIVED_JOB,RECEIVED_EVENTRANGES]

   def __init__(self,config,loop_timeout=30):
      super(DroidRequest,self).__init__()

      # here is a queue to return messages to the calling function
      self.queue        = SerialQueue.SerialQueue()

      # keep the configuration info
      self.config       = config

      # control loop execution frequency
      self.loop_timeout = loop_timeout

      # here is a way to stop the thread
      self.exit         = threading.Event()

      # current state of the thread
      self.state        = VariableWithLock.VariableWithLock(DroidRequest.CREATED)

      # message from droid that was received (None until message is received)
      self.droid_msg    = VariableWithLock.VariableWithLock()

      # droid rank which sent the message
      self.droid_rank   = VariableWithLock.VariableWithLock()

   def stop(self):
      ''' this function can be called by outside threads to cause the WorkManager thread to exit'''
      self.exit.set()

   def get_state(self):
      return self.state.get()

   def set_state(self,state):
      if state in DroidRequest.STATES:
         self.state.set(state)
      else:
         logger.error('DroidRequest: tried to set state %s which is not supported',state)
   
   def awaiting_response(self):
      if self.get_state() == DroidRequest.MESSAGE_RECEIVED and not self.queue.empty():
         return True
      return False

   def running(self):
      state = self.get_state()
      if state in DroidRequest.RUNNING_STATES:
         return True
      return False

   def received_droid_message(self):
      state = self.get_state()
      if state == DroidRequest.CREATED or state == DroidRequest.REQUESTING:
         return False
      return True

   def send_job(self,job):
      self.queue.put({'type':MessageTypes.NEW_JOB,'job':job})
      self.set_state(DroidRequest.RECEIVED_JOB)

   def send_no_more_jobs(self,job):
      self.queue.put({'type':MessageTypes.NO_MORE_JOBS})
      self.set_state(DroidRequest.RECEIVED_JOB)

   def send_eventranges(self,eventranges):
      self.queue.put({'type':MessageTypes.NEW_EVENT_RANGES,'eventranges':eventranges})
      self.set_state(DroidRequest.RECEIVED_EVENTRANGES)

   def send_no_more_eventranges(self):
      self.queue.put({'type':MessageTypes.NO_MORE_EVENT_RANGES})
      self.set_state(DroidRequest.RECEIVED_EVENTRANGES)

   def run(self):
      ''' this function is executed as the subthread. '''

      
      while not self.exit.isSet():
         logger.debug('DroidRequest: start loop')

         state = self.get_state()
         logger.debug('DroidRequest: current state: %s',state)

         # starting state
         # first thing to do is to request a message from droid ranks
         if state == DroidRequest.CREATED:
            
            # setting state
            self.set_state(DroidRequest.REQUESTING)
            # requesting message
            logger.debug('DroidRequest: requesting new message from droid')
            self.mpi_request = ydm.get_droid_message_for_workmanager()

         # after requesting an MPI message, test if the message has been received
         elif state == DroidRequest.REQUESTING:

            # test if message has been received, this loop will continue 
            # until a message is received, or the thread is signaled to exit
            status = MPI.Status()
            msg_received,msg = self.mpi_request.test(status=status)
            while not msg_received and not self.exit.wait(timeout=5):
               msg_received,msg = self.mpi_request.test(status=status)

            # if the message is received, sent the message variable and 
            if msg_received:
               logger.debug('DroidRequest: message received from droid rank %d: %s',status.Get_source(),msg)
               self.droid_msg.set(msg)
               self.droid_rank.set(status.Get_source())

               # change the state to MESSAGE_RECEIVED
               self.set_state(DroidRequest.MESSAGE_RECEIVED)
            # otherwise we do nothing
            else:
               logger.debug('DroidRequest: message not yet received')

         # while waiting for a message from WorkManager do some sleeping
         elif state == DroidRequest.MESSAGE_RECEIVED:
            time.sleep(self.loop_timeout)

         # the next step is for WorkManager to send a job or event range
         elif state == DroidRequest.RECEIVED_JOB:
            # check if WorkManager has sent a reply
            if not self.queue.empty():
               logger.debug('DroidRequest: retrieving message from work manager')
               try:
                  msg = self.queue.get(block=False)
               except SerialQueue.Empty():
                  logger.debug('DroidRequest: DroidRequest queue empty')
               else:
                  
                  droid_msg = self.droid_msg.get()
                  droid_rank = self.droid_rank.get()
                  
                  if msg['type'] == MessageTypes.NEW_JOB and droid_msg['type'] == MessageTypes.REQUEST_JOB:
                     logger.debug('DroidRequest: sending new job to droid rank %d',droid_rank)
                     ydm.send_droid_new_job(msg['job'],droid_rank).wait()
                  elif msg['type'] == MessageTypes.NO_MORE_JOBS:
                     logger.debug('DroidRequest: sending NO new job to droid rank %d',droid_rank)
                     ydm.send_droid_no_job_left(droid_rank).wait()
                  else:
                     logger.error('DroidRequest: type mismatch error: current mtype=%s, WorkManager message type=%s',current_mtype,msg['type'])
                  # in all these cases, the response is complete
                  self.stop()
                  
            else:
               logger.debug('DroidRequest: DroidRequest queue is empty')
         elif state == DroidRequest.RECEIVED_EVENTRANGES:
            # check if WorkManager has sent a reply
            if not self.queue.empty():
               logger.debug('DroidRequest: retrieving message from work manager')
               try:
                  msg = self.queue.get(block=False)
               except SerialQueue.Empty():
                  logger.debug('DroidRequest: DroidRequest queue empty')
               else:
                  droid_msg = self.droid_msg.get()
                  droid_rank = self.droid_rank.get()
                  
                  if msg['type'] == MessageTypes.NEW_EVENT_RANGES and droid_msg['type'] == MessageTypes.REQUEST_EVENT_RANGES:
                     logger.debug('DroidRequest: sending new event ranges to droid rank %d',droid_rank)
                     ydm.send_droid_new_eventranges(msg['eventranges'],droid_rank).wait()
                  elif msg['type'] == MessageTypes.NO_MORE_EVENT_RANGES:
                     logger.debug('DroidRequest: sending NO new event ranges to droid rank %d',droid_rank)
                     ydm.send_droid_no_eventranges_left(droid_rank).wait()
                  else:
                     logger.error('DroidRequest: type mismatch error: current mtype=%s, WorkManager message type=%s',current_mtype,msg['type'])
                  # in all these cases, the response is complete
                  self.stop()
                  
            else:
               logger.debug('DroidRequest: DroidRequest queue is empty')

      self.set_state(DroidRequest.EXITED)
      logger.debug('DroidRequest: DroidRequest exiting')



class DroidRequestList:
   ''' a list of DroidRequest threads to keep track of multiple requests '''

   def __init__(self):

      # internal list of droid requests
      self.droid_requests = []

   def add_request(self,config,loop_timeout=30):
      new_request = DroidRequest(config,loop_timeout)
      new_request.start()
      self.append(new_request)

   def get_running(self):
      running = []
      for req in self.droid_requests:
         if req.running():
            running.append(req)
      return running

   def get_waiting(self):
      reqs = []
      for req in self.droid_requests:
         if req.get_state() == DroidRequest.MESSAGE_RECEIVED:
            reqs.append(req)
      return reqs

   def number_running(self):
      running = 0
      for req in self.droid_requests:
         if req.running():
            running += 1
      return running

   def number_waiting_for_droid_message(self):
      waiting = 0
      for req in self.droid_requests:
         if not req.received_droid_message():
            waiting += 1
      return waiting

   def number_in_state(self,state):
      counter = 0
      if state in DroidRequest.STATES:
         for req in self.droid_requests:
            if req.get_state() == state:
               counter += 1
      return counter

   def number_created(self):
      return self.number_in_state(DroidRequest.CREATED)
   def number_requesting(self):
      return self.number_in_state(DroidRequest.REQUESTING)
   def number_message_received(self):
      return self.number_in_state(DroidRequest.MESSAGE_RECEIVED)
   def number_exited(self):
      return self.number_in_state(DroidRequest.EXITED)
   
   def number_received_response(self):
      counter = 0
      for req in self.droid_requests:
         state = req.get_state()
         if state == DroidRequest.RECEIVED_EVENTRANGES or state == DroidRequest.RECEIVED_JOB:
            counter += 1

      return counter



   def append(self,request):
      if isinstance(request,DroidRequest):
         self.droid_requests.append(request)
      else:
         raise TypeError('object is not of type DroidRequest: %s' % type(request).__name__)
   def insert(self,index,request):
      if isinstance(request,DroidRequest):
         self.droid_requests.insert(index,request)
      else:
         raise TypeError('object is not of type DroidRequest: %s' % type(request).__name__)
   def pop(self,index=None):
      return self.droid_requests.pop(index)
   def index(self,value):
      return self.droid_requests.index(value)
   def remove(self,value):
      self.droid_requests.remove(value)
   def __iter__(self):
      return iter(self.droid_requests)
   def __len__(self):
      return len(self.droid_requests)
   def __getitem__(self,index):
      return self.droid_requests[index]
   def __setitem__(self,index,value):
      if isinstance(value,DroidRequest):
         self.droid_requests[index] = value
      else:
         raise TypeError('object is not of type DroidRequest: %s' % type(value).__name__)
   def __delitem__(self,index):
      del self.droid_requests[index]
   def __contains__(self,value):
      return self.droid_requests.__contains__(value)



