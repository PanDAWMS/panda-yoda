import logging,os,sys,importlib,time,threading
from mpi4py import MPI
logger = logging.getLogger(__name__)
from pandayoda.common import StatefulService,VariableWithLock,SerialQueue
from pandayoda.common import yoda_droid_messenger as ydm,MessageTypes

class DroidRequest(StatefulService.StatefulService):
   ''' This thread manages one message from Droid ranks '''

   CREATED              = 'CREATED'
   REQUESTING           = 'REQUESTING'
   MESSAGE_RECEIVED     = 'MESSAGE_RECEIVED'
   RECEIVED_JOB         = 'RECEIVED_JOB'
   RECEIVED_EVENTRANGES = 'RECEIVED_EVENTRANGES'
   EXITED               = 'EXITED'

   STATES = [CREATED,REQUESTING,MESSAGE_RECEIVED,RECEIVED_JOB,RECEIVED_EVENTRANGES,EXITED]
   RUNNING_STATES = [CREATED,REQUESTING,MESSAGE_RECEIVED,RECEIVED_JOB,RECEIVED_EVENTRANGES]

   def __init__(self,config,msg_tag=None,loop_timeout=30,parent_name = None):
      super(DroidRequest,self).__init__(loop_timeout)

      # here is a queue to return messages to the calling function
      self.queue        = SerialQueue.SerialQueue()

      # keep the configuration info
      self.config       = config

      # the message tag to use to filter MPI messages received by this request
      self.msg_tag      = msg_tag

      # message from droid that was received (None until message is received)
      self.droid_msg    = VariableWithLock.VariableWithLock()

      # droid rank which sent the message
      self.droid_rank   = VariableWithLock.VariableWithLock()

      # name of the function or class creating this request
      self.parent_name  = parent_name

      # event to block on when waiting for a response
      self.received_response = threading.Event()

      # override the base classes prelog since we don't need the rank number for yoda.
      self.prelog       = ''
      if parent_name:
         self.prelog    = '%s|' % parent_name


   def get_droid_rank(self):
      return self.droid_rank.get()
   
   def awaiting_response(self):
      if self.get_state() == DroidRequest.MESSAGE_RECEIVED and not self.queue.empty():
         return True
      return False

   def running(self):
      if self.get_state() in DroidRequest.RUNNING_STATES:
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
      self.received_response.set()

   def send_no_more_jobs(self):
      self.queue.put({'type':MessageTypes.NO_MORE_JOBS})
      self.set_state(DroidRequest.RECEIVED_JOB)
      self.received_response.set()

   def send_eventranges(self,eventranges):
      self.queue.put({'type':MessageTypes.NEW_EVENT_RANGES,'eventranges':eventranges})
      self.set_state(DroidRequest.RECEIVED_EVENTRANGES)
      self.received_response.set()

   def send_no_more_eventranges(self):
      self.queue.put({'type':MessageTypes.NO_MORE_EVENT_RANGES})
      self.set_state(DroidRequest.RECEIVED_EVENTRANGES)
      self.received_response.set()

   def run(self):
      ''' this function is executed as the subthread. '''

      # set initial state
      self.set_state(self.CREATED)
      
      while not self.exit.isSet():
         logger.debug('%s start loop',self.prelog)

         state = self.get_state()
         logger.debug('%s current state: %s',self.prelog,state)

         ##################
         # CREATED: starting state, first thing to do is to request a message 
         #        from droid ranks
         ######################################################################
         if state == DroidRequest.CREATED:
            
            # setting state
            self.set_state(DroidRequest.REQUESTING)
            # requesting message
            logger.debug('%s requesting new message from droid with tag %s',self.prelog,self.msg_tag)
            self.mpi_request = ydm.receive_message(tag=self.msg_tag)

         
         ##################
         # REQUESTING: after requesting an MPI message, test if the message  
         #        has been received
         ######################################################################
         elif state == DroidRequest.REQUESTING:
            logger.debug('%s testing message request',self.prelog)
            # test if message has been received, this loop will continue 
            # until a message is received, or the thread is signaled to exit
            status = MPI.Status()
            msg_received,msg = self.mpi_request.test(status=status)
            
            # if the message is received, sent the message variable and 
            if msg_received:
               logger.debug('%s message received from droid rank %d: %s',self.prelog,status.Get_source(),msg)
               self.droid_msg.set(msg)
               self.droid_rank.set(status.Get_source())

               # change the state to MESSAGE_RECEIVED
               self.set_state(DroidRequest.MESSAGE_RECEIVED)
            # otherwise we do nothing
            else:
               logger.debug('%s message not yet received, sleeping %d',self.prelog,self.loop_timeout)
               time.sleep(self.loop_timeout)

         
         ##################
         # MESSAGE_RECEIVED: while waiting for a message from WorkManager
         #         do some sleeping
         ######################################################################
         elif state == DroidRequest.MESSAGE_RECEIVED:
            logger.debug('%s waiting for WorkManager to respond.',self.prelog)
            
            # if the message was a job or event range request, sleep on the queue 
            # since that is where the message will come from
            if self.received_response.wait(self.loop_timeout):
               logger.debug('%s received response, time to wake up.',self.prelog)
               self.received_response.clear()
            else:
               time.sleep(self.loop_timeout)

         
         ##################
         # RECEIVED_JOB: the next step is for WorkManager to send a job 
         #           or event range
         ######################################################################
         elif state == DroidRequest.RECEIVED_JOB:
            # check if WorkManager has sent a reply
            if not self.queue.empty():
               logger.debug('%s retrieving message from work manager',self.prelog)
               try:
                  msg = self.queue.get(block=False)
               except SerialQueue.Empty():
                  logger.debug('%s DroidRequest queue empty',self.prelog)
               else:
                  
                  droid_msg = self.droid_msg.get()
                  droid_rank = self.droid_rank.get()
                  
                  if msg['type'] == MessageTypes.NEW_JOB and droid_msg['type'] == MessageTypes.REQUEST_JOB:
                     logger.debug('%s sending new job to droid rank %d',self.prelog,droid_rank)
                     ydm.send_droid_new_job(msg['job'],droid_rank).wait()
                  elif msg['type'] == MessageTypes.NO_MORE_JOBS:
                     logger.debug('%s sending NO new job to droid rank %d',self.prelog,droid_rank)
                     ydm.send_droid_no_job_left(droid_rank).wait()
                  else:
                     logger.error('%s type mismatch error: current mtype=%s, WorkManager message type=%s',self.prelog,current_mtype,msg['type'])
                  # in all these cases, the response is complete
                  self.stop()
                  
            else:
               logger.debug('%s DroidRequest queue is empty',self.prelog)
         
         ##################
         # RECEIVED_EVENTRANGES: the next step is for WorkManager to send a job 
         #           or event range
         ######################################################################
         elif state == DroidRequest.RECEIVED_EVENTRANGES:
            # check if WorkManager has sent a reply
            if not self.queue.empty():
               logger.debug('%s retrieving message from work manager',self.prelog)
               try:
                  msg = self.queue.get(block=False)
               except SerialQueue.Empty():
                  logger.debug('%s DroidRequest queue empty',self.prelog)
               else:
                  droid_msg = self.droid_msg.get()
                  droid_rank = self.droid_rank.get()
                  
                  if msg['type'] == MessageTypes.NEW_EVENT_RANGES and droid_msg['type'] == MessageTypes.REQUEST_EVENT_RANGES:
                     logger.debug('%s sending new event ranges to droid rank %d',self.prelog,droid_rank)
                     ydm.send_droid_new_eventranges(msg['eventranges'],droid_rank).wait()
                  elif msg['type'] == MessageTypes.NO_MORE_EVENT_RANGES:
                     logger.debug('%s sending NO new event ranges to droid rank %d',self.prelog,droid_rank)
                     ydm.send_droid_no_eventranges_left(droid_rank).wait()
                  else:
                     logger.error('%s type mismatch error: current mtype=%s, WorkManager message type=%s',self.prelog,current_mtype,msg['type'])
                  # in all these cases, the response is complete
                  self.stop()
                  
            else:
               logger.debug('%s DroidRequest queue is empty',self.prelog)

         else:
            logger.error('%s Failed to parse the current state: %s',self.prelog,state)

      self.set_state(DroidRequest.EXITED)
      logger.debug('%s DroidRequest exiting',self.prelog)