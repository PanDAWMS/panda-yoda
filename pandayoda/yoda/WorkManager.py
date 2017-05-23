import os,sys,threading,logging
from pandayoda.common import MessageTypes,SerialQueue,yoda_droid_messenger as ydm
logger = logging.getLogger(__name__)

class WorkManager(threading.Thread):
   ''' Work Manager: this thread manages work going to the running Droids '''

   def __init__(self,queues,config
                loopTimeout      = 30):
      ''' 
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send 
                     messages to other Droid components about errors, etc.
        config: the ConfigParser handle for yoda
        loopTimeout: A positive number of seconds to block when retrieving 
                     a message from the inputQueue.'''
      # call base class init function
      super(WorkManager,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # configuration of Yoda
      self.config          = config

      # the timeout duration 
      self.loopTimeout           = loopTimeout

      # this is used to trigger the thread exit
      self.exit = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the WorkManager thread to exit'''
      self.exit.set()

   def run(self):
      ''' this function is executed as the subthread. '''

      # create these instance variables here so they are not created in the parent thread, saves memory
      self.job_list = []
      self.event_per_job = {}
      self.eventranges = []
      self.eventranges_todo = []
      self.eventranges_done = []

      self.pending_msgs = []

      while not self.exit.isSet():
         logger.debug('start loop')

         # first check that there is work to be done
         if self.have_work():
            # have work so just wait for droid to request work
            # wait for message from droid ranks
            try:
               msg = self.queues['WorkManager'].get(timeout=self.loopTimeout)
            except SerialQueue.Empty:
               logger.debug('no message on WorkManager queue')
            else:
               logger.debug('received message from WorkManager queue: %s',msg)

               # act on message
               if msg['type'] == MessageTypes.REQUEST_JOB:
                  
                  # check currently have job
                  self.get_job()
                  

                  # reply with job
                  reply = {'type':MessageTypes.NEW_JOB,'job':job,'source_rank':msg['source_rank']}
                  self.queues['DroidComm'].put(reply)

         else:
            # send message to harvester to send work
            self.get_work()

   def have_work(self):
      return False

   def get_work(self):

      self.get_job()

      self.get_eventranges()

   def get_job(self):

      for job in job_list:
         if job['PandaID'] in self.eventranges_todo:
            if len(self.eventranges_todo[job['PandaID']]) > 










