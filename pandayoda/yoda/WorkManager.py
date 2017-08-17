import os,sys,threading,logging,importlib,time
import RequestHarvesterJob,RequestHarvesterEventRanges
from pandayoda.common import MessageTypes,SerialQueue,EventRangeList,exceptions
from pandayoda.common import yoda_droid_messenger as ydm,VariableWithLock,StatefulService
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
      ''' this function can be called by outside subthreads to cause the JobManager thread to exit'''
      self.exit.set()


   def run(self):
      ''' this function is executed as the subthread. '''

      # read inputs from config file
      self.read_config()

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
      subthreads = {}
      subthreads['RequestHarvesterJob']  = RequestHarvesterJob.RequestHarvesterJob(self.config,self.loop_timeout)
      subthreads['RequestHarvesterJob'].start()

      subthreads['RequestHarvesterEventRanges']  = RequestHarvesterEventRanges.RequestHarvesterEventRanges(self.config,self.loop_timeout)
      subthreads['RequestHarvesterEventRanges'].start()
      

      while not self.exit.isSet():
         logger.debug('start loop')
         logger.debug('cwd: %s',os.getcwd())

         ###############
         # check all subthreads still running
         ################################
         logger.debug('checking all subthreads are still running and if they have messages')
         for name,thread in subthreads.iteritems():
            if not thread.isAlive() or thread.in_state(thread.EXITED):
               logger.error('%s has exited',name)

         ###############
         # check for a new job
         ################################
         logger.debug('checking for new job')
         if subthreads['RequestHarvesterJob'].new_jobs_ready():
            logger.debug('getting new jobs from RequestHarvesterJob')
            new_jobs = subthreads['RequestHarvesterJob'].get_jobs()
            # add new_jobs to list
            for jobid,job in new_jobs.iteritems():
               # add job to list of jobs orderd by job id
               pandajobs[jobid] = job
         elif subthreads['RequestHarvesterJob'].no_more_jobs():
            logger.debug('there are no more jobs')
         elif not subthreads['RequestHarvesterJob'].isAlive():
            logger.debug('RequestHarvesterJob thread has exited.')
         else:
            logger.debug('RequestHarvesterJob state is %s',subthreads['RequestHarvesterJob'].get_state())



         ###############
         # check for a new event range
         ################################
         logger.debug('checking for event range')
         if subthreads['RequestHarvesterEventRanges'].new_eventranges_ready():
            logger.debug('retrieving event range from RequestHarvesterEventRanges')
            new_eventranges = subthreads['RequestHarvesterEventRanges'].get_eventranges()
            if new_eventranges is None:
               logger.error('new eventranges ready set, but none available. Should not happen.')
            else:
               # parse event ranges if they were returned
               if len(new_eventranges) > 0: 
                  # add new event ranges to the list
                  for jobid,list_of_eventranges in new_eventranges.iteritems():
                     logger.debug('Adding %d event ranges to the list for job id %s',len(list_of_eventranges),jobid)
                     if jobid in eventranges:
                        eventranges[str(jobid)] += EventRangeList.EventRangeList(list_of_eventranges)
                     else:
                        eventranges[str(jobid)] = EventRangeList.EventRangeList(list_of_eventranges)
         elif subthreads['RequestHarvesterEventRanges'].no_more_eventranges():
            logger.debug('there are no more event ranges')
         elif not subthreads['RequestHarvesterEventRanges'].isAlive():
            logger.debug('RequestHarvesterEventRanges thread has exited.')
         else:
            logger.debug('RequestHarvesterEventRanges state is %s',subthreads['RequestHarvesterEventRanges'].get_state())
         


         ################
         # check for queue messages
         ################################
         try:
            qmsg = self.queues['WorkManager'].get(block=False)
         except SerialQueue.Empty():
            logger.debug('WorkManager queue is empty')
         else:
            logger.debug('received message %s',qmsg)


            #############
            ## DROID requesting new job
            ###############################
            if qmsg['type'] == MessageTypes.REQUEST_JOB:
               logger.debug('droid requesting job description')
               # check to see what events I have left to do and to which job they belong
               # then decide which job description to send the droid rank

               # if there are no jobs in the list need to make sure one is being requested
               if len(pandajobs) == 0:
                  # if there are no event ranges and RequestHarvesterJob thread is idle, start a new request
                  logger.debug('no panda jobs')
                  if len(eventranges) == 0:
                     logger.debug('no eventranges')
                     if subthreads['RequestHarvesterJob'].idle():
                        logger.debug('starting new request for RequestHarvesterJob')
                        subthreads['RequestHarvesterJob'].start_request()

                     else:
                        logger.debug('There are no jobs or eventranges, but the RequestHarvesterJob thread is not idle, so waiting for jobs')

                  else:
                     logger.warning('no jobs but have eventranges, perhaps communication is out of sync. Will try waiting for next loop.')


                  # place message back on queue for later processing
                  self.queues['WorkManager'].put(qmsg)
               
               # if there are jobs, need to pick one.
               else:
                  logger.debug('have panda jobs')
                  # if there are no eventranges, assume that the first job has been received and reply with the only job available
                  if len(eventranges) == 0:
                     logger.debug('no event ranges')
                     if len(pandajobs) == 1:
                        # extract the panda job id
                        pandaid = pandajobs.keys()[0]
                        logger.debug('sending droid rank %s panda id %s',qmsg['source_rank'],pandaid)
                        outmsg = {
                           'type':MessageTypes.NEW_JOB,
                           'job':pandajobs[pandaid],
                           'destination_rank':qmsg['source_rank']
                        }
                        self.queues['MPIService'].put(outmsg)

                     else:
                        logger.warning('have more than one panda job, but no eventranges. This may be an error, or harvester is going to be giving yoda eventranges from more than one jobs. Have not implemented this case.')
                        # place message back on queue for later processing
                        self.queues['WorkManager'].put(qmsg)
                  else:
                     logger.debug('determining which of the %s panda ids to send to droid rank %s',len(pandajobs),qmsg['source_rank'])
                     # find the panda job with the most event ranges ready
                     most_ready = 0
                     most_ready_id = None
                     for pandaid,ranges in eventranges.iteritems():
                        if ranges.number_ready() > most_ready:
                           most_ready = ranges.number_ready()
                           most_ready_id = pandaid

                     if most_ready_id is None:
                        logger.debug('there are no events ready')
                        if subthreads['RequestHarvesterJob'].no_more_jobs():
                           logger.debug('sending no more jobs')
                           outmsg = {
                              'type':MessageTypes.NO_MORE_JOBS,
                              'destination_rank':qmsg['source_rank'],
                           }
                           self.queues['MPIService'].put(outmsg)
                        elif subthreads['RequestHarvesterJob'].idle():
                           logger.debug('start a new request')
                           subthreads['RequestHarvesterJob'].start_request()
                           # place message back on queue for later processing
                           self.queues['WorkManager'].put(qmsg)   
                        else:
                           logger.debug('waiting for job or events to arrive')
                           # place message back on queue for later processing
                           self.queues['WorkManager'].put(qmsg)
                     else:
                        logger.debug('sending droid rank %s panda id %s which has the most ready events %s',request.get_droid_rank(),most_ready_id,most_ready)
                        outmsg = {
                           'type':MessageTypes.NEW_JOB,
                           'job':pandajobs[most_ready_id],
                           'destination_rank':qmsg['source_rank']
                        }
                        self.queues['MPIService'].put(outmsg)
                        
                  
            
            #############
            ## DROID requesting new event ranges
            ###############################
            elif qmsg['type'] == MessageTypes.REQUEST_EVENT_RANGES:
               logger.debug('droid requesting event ranges')

               # check the job definition which is already running on this droid rank
               # see if there are more events to be dolled out

               # send the number of event ranges equal to the number of Athena workers

               # the droid sent the current running panda id, determine if there are events left for this panda job
               droid_pandaid = msg['pandaID']

               if str(droid_pandaid) not in eventranges:
                  logger.debug('droid rank %s is requesting events, but none for panda id %s have been received. Current ids: %s',qmsg['source_rank'],droid_pandaid,str(eventranges.keys()))
                  if subthreads['RequestHarvesterEventRanges'].idle():
                     logger.debug('RequestHarvesterEventRanges starting new request')
                     subthreads['RequestHarvesterEventRanges'].start_request(
                           {
                            'pandaID':qmsg['pandaID'],
                            'jobsetID':qmsg['jobsetID'],
                            'taskID':qmsg['taskID'],
                            'nRanges':self.request_n_eventranges,
                           }
                        )
               elif eventranges[str(droid_pandaid)].number_ready() > 0:
                  if eventranges[str(droid_pandaid)].number_ready() >= self.send_n_eventranges:
                     local_eventranges = eventranges[str(droid_pandaid)].get_next(self.send_n_eventranges)
                  else:
                     local_eventranges = eventranges[str(droid_pandaid)].get_next(eventranges[str(droid_pandaid)].number_ready())
                  
                  # send event ranges to DroidRequest
                  logger.debug('sending %d new event ranges to droid rank %d',len(local_eventranges),qmsg['source_rank'])
                  outmsg = {
                     'type':MessageTypes.NEW_EVENT_RANGES,
                     'eventranges': local_eventranges,
                     'destination_rank':qmsg['source_rank'],
                  }
                  self.queues['MPIService'].put(outmsg)
               else:
                  logger.debug('droid request asking for eventranges, but no event ranges left in local list')
                  if subthreads['RequestHarvesterEventRanges'].idle():
                     logger.debug('Setting RequestHarvesterEventRanges state to REQUEST to trigger new request')
                     subthreads['RequestHarvesterEventRanges'].start_request(
                           {
                            'pandaID':msg['pandaID'],
                            'jobsetID':msg['jobsetID'],
                            'taskID':msg['taskID'],
                            'nRanges':self.request_n_eventranges,
                           }
                        )
                  elif subthreads['RequestHarvesterEventRanges'].exited() and subthreads['RequestHarvesterEventRanges'].no_more_eventranges():
                     logger.debug('GetEventRanges has exited and no more ranges to get')
                     outmsg = {
                        'type':MessageTypes.NO_MORE_EVENT_RANGES,
                        'destination_rank':qmsg['source_rank'],
                     }
                     self.queues['MPIService'].put(outmsg)

            else:
               logger.error('message type was not recognized: %s',msg['type'])
         
         
         

         # if there is nothing to be done, sleep
         if not subthreads['RequestHarvesterJob'].new_jobs_ready() and \
            not subthreads['RequestHarvesterEventRanges'].new_eventranges_ready() and \
            self.queues['WorkManager'].empty():
            time.sleep(self.loop_timeout)
         else:
            logger.debug('continuing loop: %s %s %s',not subthreads['RequestHarvesterJob'].new_jobs_ready(),not subthreads['RequestHarvesterEventRanges'].new_eventranges_ready(),self.queues['WorkManager'].empty())
            #time.sleep(1)

      for name,thread in subthreads.iteritems():
         logger.info('signaling exit to thread %s',name)
         thread.stop()
      for name,thread in subthreads.iteritems():
         logger.info('waiting for %s to join',name)
         thread.join()
         logger.info('%s has joined',name)

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

   def read_config(self):
      # get self.loop_timeout
      if self.config.has_option(config_section,'self.loop_timeout'):
         self.loop_timeout = self.config.getfloat(config_section,'self.loop_timeout')
      else:
         logger.error('must specify "self.loop_timeout" in "%s" section of config file',config_section)
         return
      logger.info('self.loop_timeout: %d',self.loop_timeout)


      # get self.send_n_eventranges
      if self.config.has_option(config_section,'self.send_n_eventranges'):
         self.send_n_eventranges = self.config.getint(config_section,'self.send_n_eventranges')
      else:
         logger.error('must specify "self.send_n_eventranges" in "%s" section of config file',config_section)
         return
      logger.info('self.send_n_eventranges: %d',self.send_n_eventranges)

      # get self.request_n_eventranges
      if self.config.has_option(config_section,'self.request_n_eventranges'):
         self.request_n_eventranges = self.config.getint(config_section,'self.request_n_eventranges')
      else:
         logger.error('must specify "self.request_n_eventranges" in "%s" section of config file',config_section)
         return
      logger.info('self.request_n_eventranges: %d',self.request_n_eventranges)







