import os,sys,threading,logging,importlib,time
import RequestHarvesterJob,RequestHarvesterEventRanges,DroidRequestList
from mpi4py import MPI
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

      # get request_n_eventranges
      if self.config.has_option(config_section,'request_n_eventranges'):
         request_n_eventranges = self.config.getint(config_section,'request_n_eventranges')
      else:
         logger.error('must specify "request_n_eventranges" in "%s" section of config file',config_section)
         return
      logger.info('request_n_eventranges: %d',request_n_eventranges)

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
      threads['RequestHarvesterJob']  = RequestHarvesterJob.RequestHarvesterJob(self.config,loop_timeout)
      threads['RequestHarvesterJob'].start()

      threads['RequestHarvesterEventRanges']  = RequestHarvesterEventRanges.RequestHarvesterEventRanges(self.config,loop_timeout)
      threads['RequestHarvesterEventRanges'].start()
      
      # there can be multiple droid requests running at any given time.
      # this list keeps track of them
      droid_requests = DroidRequestList.DroidRequestList(self.config,ydm.TO_YODA_WORKMANAGER,loop_timeout,self.__class__.__name__)

      while not self.exit.isSet():
         logger.debug('start loop')
         logger.debug('cwd: %s',os.getcwd())

         logger.debug('checking all threads are still running and if they have messages')
         for name,thread in threads.iteritems():
            if not thread.isAlive() or thread.in_state(thread.EXITED):
               logger.error('%s has exited',name)

         ###############
         # check for a new job
         ################################
         logger.debug('checking for new job')
         if threads['RequestHarvesterJob'].new_jobs_ready():
            logger.debug('getting new jobs from RequestHarvesterJob')
            new_jobs = threads['RequestHarvesterJob'].get_jobs()
            # add new_jobs to list
            for jobid,job in new_jobs.iteritems():
               # add job to list of jobs orderd by job id
               pandajobs[jobid] = job
         elif threads['RequestHarvesterJob'].no_more_jobs():
            logger.debug('there are no more jobs')
         else:
            logger.debug('RequestHarvesterJob state is %s',threads['RequestHarvesterJob'].get_state())



         ###############
         # check for a new event range
         ################################
         logger.debug('checking for event range')
         if threads['RequestHarvesterEventRanges'].new_eventranges_ready():
            logger.debug('retrieving event range from GetEventRanges queue')
            new_eventranges = threads['RequestHarvesterEventRanges'].get_eventranges()
            if new_eventranges is None:
               logger.error('new eventranges ready set, but none available')
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
         elif threads['RequestHarvesterEventRanges'].no_more_eventranges():
            logger.debug('there are no more event ranges')
         else:
            logger.debug('RequestHarvesterEventRanges state is %s',threads['RequestHarvesterEventRanges'].get_state())
            

         ################
         # check for Droid requests
         ################################
         logger.debug('checking for droid messages, current request count: %s',len(droid_requests))

         # process the droid requests that are waiting for a response
         for request in droid_requests.get_waiting():
            # get the message
            msg = request.droid_msg.get()

            logger.debug('message received from droid rank %s: %s',request.get_droid_rank(),msg)

            #############
            ## DROID requesting new job
            ###############################
            if msg['type'] == MessageTypes.REQUEST_JOB:
               logger.debug('droid requesting job description')
               # check to see what events I have left to do and to which job they belong
               # then decide which job description to send the droid rank

               # if there are no jobs in the list need to make sure one is being requested
               if len(pandajobs) == 0:
                  # if there are no event ranges and RequestHarvesterJob thread is idle, start a new request
                  logger.debug('no panda jobs')
                  if len(eventranges) == 0:
                     logger.debug('no eventranges')
                     if threads['RequestHarvesterJob'].idle():
                        logger.debug('starting new request for RequestHarvesterJob')
                        threads['RequestHarvesterJob'].start_request()
                     else:
                        logger.debug('There are no jobs or eventranges, but the RequestHarvesterJob thread is not idle, so waiting for jobs')
                  else:
                     logger.warning('no jobs but have eventranges, perhaps communication is out of sync. Will try waiting for next loop.')
               
               # if there are jobs, need to pick one.
               else:
                  logger.debug('have panda jobs')
                  # if there are no eventranges, assume that the first job has been received and reply with the only job available
                  if len(eventranges) == 0:
                     logger.debug('no event ranges')
                     if len(pandajobs) == 1:
                        pandaid = pandajobs.keys()[0]
                        logger.debug('sending droid rank %s panda id %s',request.get_droid_rank(),pandaid)
                        request.send_job(pandajobs[pandaid])
                     else:
                        logger.warning('have more than one panda job, but no eventranges. This may be an error, or harvester is going to be giving yoda eventranges from more than one jobs. Have not implemented this case.')
                  else:
                     logger.debug('determining which of the %s panda ids to send to droid rank %s',len(pandajobs),request.get_droid_rank())
                     # find the panda job with the most event ranges ready
                     most_ready = 0
                     most_ready_id = None
                     for pandaid,ranges in eventranges.iteritems():
                        if ranges.number_ready() > most_ready:
                           most_ready = ranges.number_ready()
                           most_ready_id = pandaid

                     if most_ready_id is None:
                        logger.debug('there are no events ready')
                        if threads['RequestHarvesterJob'].no_more_jobs():
                           logger.debug('sending no more jobs')
                           request.send_no_more_jobs()
                        elif threads['RequestHarvesterJob'].idle():
                           logger.debug('start a new request')
                           threads['RequestHarvesterJob'].start_request()
                        else:
                           logger.debug('waiting for job or events to arrive')
                     else:
                        logger.debug('sending droid rank %s panda id %s which has the most ready events %s',request.get_droid_rank(),most_ready_id,most_ready)
                        request.send_job(pandajobs[most_ready_id])
                  
            
            #############
            ## DROID requesting new event ranges
            ###############################
            elif msg['type'] == MessageTypes.REQUEST_EVENT_RANGES:
               logger.debug('droid requesting event ranges')

               # check the job definition which is already running on this droid rank
               # see if there are more events to be dolled out

               # send the number of event ranges equal to the number of Athena workers

               # the droid sent the current running panda id, determine if there are events left for this panda job
               droid_jobid = msg['pandaID']

               if str(droid_jobid) not in eventranges:
                  logger.debug('droid rank %s is requesting events, but none for panda id %s have been received. Current ids: %s',request.get_droid_rank(),droid_jobid,str(eventranges.keys()))
                  if threads['RequestHarvesterEventRanges'].idle():
                     logger.debug('RequestHarvesterEventRanges starting new request')
                     threads['RequestHarvesterEventRanges'].start_request(
                           {
                            'pandaID':msg['pandaID'],
                            'jobsetID':msg['jobsetID'],
                            'taskID':msg['taskID'],
                            'nRanges':request_n_eventranges,
                           }
                        )
               elif eventranges[str(droid_jobid)].number_ready() > 0:
                  if eventranges[str(droid_jobid)].number_ready() >= send_n_eventranges:
                     local_eventranges = eventranges[str(droid_jobid)].get_next(send_n_eventranges)
                  else:
                     local_eventranges = eventranges[str(droid_jobid)].get_next(eventranges[str(droid_jobid)].number_ready())
                  
                  # send event ranges to DroidRequest
                  logger.debug('sending %d new event ranges to droid rank %d',len(local_eventranges),request.get_droid_rank())
                  request.send_eventranges(local_eventranges)
               else:
                  logger.debug('droid request asking for eventranges, but no event ranges left in local list')
                  if threads['RequestHarvesterEventRanges'].idle():
                     logger.debug('Setting GetEventRanges state to REQUEST to trigger new request')
                     threads['RequestHarvesterEventRanges'].start_request(
                           {
                            'pandaID':msg['pandaID'],
                            'jobsetID':msg['jobsetID'],
                            'taskID':msg['taskID'],
                            'nRanges':request_n_eventranges,
                           }
                        )
                  elif threads['RequestHarvesterEventRanges'].exited() and threads['RequestHarvesterEventRanges'].no_more_eventranges():
                     logger.debug('GetEventRanges has exited and no more ranges to get')
                     request.send_no_more_eventranges()

            else:
               logger.error('message type was not recognized: %s',msg['type'])
         
         # if no droid requests are waiting for droid messages, create a new one
         if droid_requests.number_waiting_for_droid_message() <= 0:
            droid_requests.add_request()
         

         # if there is nothing to be done, sleep
         if not threads['RequestHarvesterJob'].new_jobs_ready() and \
            not threads['RequestHarvesterEventRanges'].new_eventranges_ready() and \
            len(droid_requests.get_waiting()) == 0:
            time.sleep(loop_timeout)
         else:
            logger.debug('continuing loop: %s %s %s',not threads['RequestHarvesterJob'].new_jobs_ready(),not threads['RequestHarvesterEventRanges'].new_eventranges_ready(),len(droid_requests.get_waiting()) == 0)
            time.sleep(1)

      for name,thread in threads.iteritems():
         logger.info('signaling exit to thread %s',name)
         thread.stop()
      for name,thread in threads.iteritems():
         logger.info('waiting for %s to join',name)
         thread.join()

      logger.info('check that no DroidRequests need to be exited')
      for request in droid_requests.get_alive():
         request.stop()
      for request in droid_requests.get_alive():
         request.join()
      

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









