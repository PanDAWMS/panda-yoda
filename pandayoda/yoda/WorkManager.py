import os,sys,threading,logging,importlib,time
import RequestHarvesterJob,RequestHarvesterEventRanges,PandaJobDict
from pandayoda.common import MessageTypes,SerialQueue,EventRangeList,exceptions
from pandayoda.common import VariableWithLock,StatefulService
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
      
      # list of all jobs received from Harvester key-ed by panda id
      pandajobs                  = PandaJobDict.PandaJobDict()

      
      # flag to indicate there are no more event ranges
      no_more_event_ranges       = False
      # this is a place holder for the droid message request
      droid_msg_request          = None

      # pending requests from droid ranks
      self.pending_requests      = SerialQueue.SerialQueue()

      # start a Request Havester Job thread to begin getting a job
      requestHarvesterJob = RequestHarvesterJob.RequestHarvesterJob(self.config,self.loop_timeout)
      requestHarvesterJob.start()

      # place holder for Request Harevester Event Ranges instance (wait for job definition before launching)
      requestHarvesterEventRanges = None
      

      while not self.exit.isSet():
         logger.debug('start loop')

         '''
         ###############
         # check for a new job
         ################################
         logger.debug('checking for new job')
         if requestHarvesterJob is not None and not requestHarvesterJob.is_alive():
            logger.debug('retrieving new job from requestHarvesterJob')
            jobs = requestHarvesterJob.get_jobs()
            if jobs is not None:
               # add new_jobs to list
               for jobid,job in new_jobs.iteritems():
                  # add job to list of jobs keyed by job id
                  pandajobs[jobid] = job

            elif requestHarvesterJob.no_more_jobs():
               logger.debug('there are no more jobs')

            # reset request object
            requestHarvesterJob = None
            
            else:
               logger.debug('RequestHarvesterJob state is %s',requestHarvesterJob.get_state())
         else:
            logger.debug('job request is %s',)
         '''

         '''
         ###############
         # check for a new event range
         ################################
         logger.debug('checking for event range')
         if requestHarvesterEventRanges is not None and not requestHarvesterEventRanges.is_alive():
            logger.debug('retrieving event range from RequestHarvesterEventRanges')
            new_eventranges = requestHarvesterEventRanges.get_eventranges()
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
               else:
                  logger.error('no event ranges returned: %s',new_eventranges)
            # reset variable
            requestHarvesterEventRanges = None
         elif requestHarvesterEventRanges.no_more_eventranges():
            logger.debug('there are no more event ranges')
         else:
            logger.debug('RequestHarvesterEventRanges state is %s',requestHarvesterEventRanges.get_state())
         '''


         ################
         # check for queue messages
         ################################
         qmsg = None
         if not self.queues['WorkManager'].empty():
            qmsg = self.queues['WorkManager'].get(block=False)
         # if main queue is empty, process pending message queues
         elif not pending_requests.empty():
            qmsg = pending_requests.get(block=False)
         
         if qmsg:
            logger.debug('received message %s',qmsg)

            #############
            ## DROID requesting new job
            ###############################
            if qmsg['type'] == MessageTypes.REQUEST_JOB:
               logger.debug('droid requesting job description')

               # Do I have a panda job to give out?
               # if not, create a new request if no request is active
               if len(pandajobs) == 0:
                  # if there are no panda jobs and requestHarvesterJob is None, then start a request
                  # this really should never happen.
                  logger.debug('There are no panda jobs')
                  if requestHarvesterJob is None:
                     logger.info('launching new job request')
                     requestHarvesterJob = RequestHarvesterJob.RequestHarvesterJob(self.config,self.loop_timeout)
                     requestHarvesterJob.start()
                  else:
                     if requestHarvesterJob.running():
                        logger.debug('request is running, adding message to pending and will process again later')
                        pending_requests.put(qmsg)
                     elif requestHarvesterJob.exited():
                        logger.debug('request has exited')
                        jobs = requestHarvesterJob.get_jobs()
                        if jobs is None:
                           logger.info(' request has exited and returned no events, restarting request')
                           requestHarvesterJob = RequestHarvesterJob.RequestHarvesterJob(self.config,self.loop_timeout)
                           requestHarvesterJob.start()
                           pending_requests.put(qmsg)
                        else:
                           logger.debug('new jobs ready, adding to PandaJobDict, then add to pending requests')
                           pandajobs.append_from_dict(jobs)
                           # add to pending requests because the job to send will be chose by another
                           # section of code below which sends the job based on the event ranges on hand
                           pending_requests.put(qmsg)

                     else:
                        logger.error('request is stuck in state %s recreating it.',requestHarvesterJob.get_state())
                        requestHarvesterJob = RequestHarvesterJob.RequestHarvesterJob(self.config,self.loop_timeout)
                        requestHarvesterJob.start()
                        pending_requests.put(qmsg)
               
               # There are jobs in the list so choose one to send
               # The choice depends on numbers of events available for each job
               elif len(pandajobs) == 1:
                  logger.debug('There is one job so send it.')
                  
                  # get the job
                  pandaid = pandajobs.keys()[0]
                  job = pandajobs[pandaid]

                  # send it to droid rank
                  logger.debug('sending droid rank %s panda id %s which has the most ready events %s',
                              request.get_droid_rank(),pandaid,job.events_ready())
                  outmsg = {
                     'type':MessageTypes.NEW_JOB,
                     'job':job.job_def,
                     'destination_rank':qmsg['source_rank']
                  }
                  self.queues['MPIService'].put(outmsg)
                     
                  qmsg = None

               
               # There are jobs in the list so choose one to send
               # The choice depends on numbers of events available for each job
               elif len(pandajobs) > 1:
                  logger.error('there are multiple jobs to choose from, this is not yet implimented')
                  raise Exception('there are multiple jobs to choose from, this is not yet implimented')


            
            #############
            ## DROID requesting new event ranges
            ###############################
            elif qmsg['type'] == MessageTypes.REQUEST_EVENT_RANGES:
               logger.debug('droid requesting event ranges')

               # the droid sent the current running panda id, determine if there are events left for this panda job
               droid_pandaid = qmsg['pandaID']
               # get eventranges for this pandaID
               eventranges = pandajobs.get_eventranges(droid_pandaid)
               # event ranges found for pandaID
               if eventranges:
                  logger.debug('retrieved eventranges for pandaID %s, events ready %s',droid_pandaid,eventranges.number_ready())
                  if eventranges.number_ready() > 0:
                     # get a  subset of ready eventranges up to the send_n_eventranges value
                     local_eventranges = eventranges.get_next(max(eventranges.number_ready(),self.send_n_eventranges))
                     
                     # send event ranges to Droid
                     logger.debug('sending %d new event ranges to droid rank %d',len(local_eventranges),qmsg['source_rank'])
                     outmsg = {
                        'type':MessageTypes.NEW_EVENT_RANGES,
                        'eventranges': local_eventranges,
                        'destination_rank':qmsg['source_rank'],
                     }
                     self.queues['MPIService'].put(outmsg)

                     qmsg = None
                  else:
                     logger.debug('no eventranges remain for pandaID: %s',droid_pandaid)
                     
                     if requestHarvesterEventRanges:
                        logger.debug('requestHarvesterEventRanges exists')
                        if requestHarvesterEventRanges.running():
                           logger.debug('requestHarvesterEventRanges is running, will pend this request and check again')
                           pending_requests.put(qmsg)
                        elif requestHarvesterEventRanges.exited():
                           logger.debug('requestHarvesterEventRanges exited, will check for new event ranges')
                           eventranges = requestHarvesterEventRanges.get_eventranges()
                           if eventranges:
                              pandaID = requestHarvesterEventRanges.job_def['pandaID']
                              if pandaID not in pandajobs:
                                 logger.error('received Droid request for event ranges using pandaID %s but this ID is not in the pandajobs list %s, this should not be possible.',pandaID,pandajobs.keys())
                              else:
                                 pandajobs[pandaID].eventranges += EventRangeList.EventRangeList(eventranges)
                                 pending_requests.put(qmsg)
                           else:
                              logger.error('no eventranges after requestHarvesterEventRanges exited, starting new request')
                              requestHarvesterEventRanges = RequestHarvesterEventRanges.RequestHarvesterEventRanges(
                                    self.config,
                                    {
                                     'pandaID':pandajobs[droid_pandaid]['pandaID'],
                                     'jobsetID':pandajobs[droid_pandaid]['jobsetID'],
                                     'taskID':pandajobs[droid_pandaid]['taskID'],
                                     'nRanges':self.request_n_eventranges,
                                    }
                                 )
                              requestHarvesterEventRanges.start()
                              pending_requests.put(qmsg)
                        else:
                           logger.error('requestHarvesterEventRanges is in strange state %s, restarting',requestHarvesterEventRanges.get_state())
                           requestHarvesterEventRanges = RequestHarvesterEventRanges.RequestHarvesterEventRanges(
                                 self.config,
                                 {
                                  'pandaID':pandajobs[droid_pandaid]['pandaID'],
                                  'jobsetID':pandajobs[droid_pandaid]['jobsetID'],
                                  'taskID':pandajobs[droid_pandaid]['taskID'],
                                  'nRanges':self.request_n_eventranges,
                                 }
                              )
                           requestHarvesterEventRanges.start()
                           pending_requests.put(qmsg)

                     else:
                        logger.debug('requestHarvesterEventRanges does not exist, creating new request')
                        requestHarvesterEventRanges = RequestHarvesterEventRanges.RequestHarvesterEventRanges(
                              self.config,
                              {
                               'pandaID':pandajobs[droid_pandaid]['pandaID'],
                               'jobsetID':pandajobs[droid_pandaid]['jobsetID'],
                               'taskID':pandajobs[droid_pandaid]['taskID'],
                               'nRanges':self.request_n_eventranges,
                              }
                           )
                        requestHarvesterEventRanges.start()
                        pending_requests.put(qmsg)


               else:
                  logger.error('there is no eventrange for pandaID %s, this should be impossible since every pandaID in the pandajobs dictionary gets an empty EventRangeList object. Something is amiss.',droid_pandaid)

            else:
               logger.error('message type was not recognized: %s',qmsg['type'])
         
         
         

         # if there is nothing to be done, sleep
         if ( requestHarvesterJob and requestHarvesterJob.running() ) and \
            ( requestHarvesterEventRanges and requestHarvesterEventRanges.running() ) and \
               self.queues['WorkManager'].empty() and pending_requests.empty():
            time.sleep(self.loop_timeout)
         else:
            logger.debug('continuing loop')
            #time.sleep(1)

      logger.info('signaling exit to threads')
      if requestHarvesterJob and requestHarvesterJob.is_alive():
         logger.debug('signaling requestHarvesterJob to stop')
         requestHarvesterJob.stop()
      if requestHarvesterEventRanges and requestHarvesterEventRanges.is_alive():
         logger.debug('signaling requestHarvesterEventRanges to stop')
         requestHarvesterEventRanges.stop()

      if requestHarvesterJob and requestHarvesterJob.is_alive():
         logger.debug('waiting for requestHarvesterJob to join')
         requestHarvesterJob.join()
      if requestHarvesterEventRanges and requestHarvesterEventRanges.is_alive():
         logger.debug('waiting for requestHarvesterEventRanges to join')
         requestHarvesterEventRanges.join()

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
      if self.config.has_option(config_section,'loop_timeout'):
         self.loop_timeout = self.config.getfloat(config_section,'loop_timeout')
      else:
         logger.error('must specify "loop_timeout" in "%s" section of config file',config_section)
         return
      logger.info('loop_timeout: %d',self.loop_timeout)


      # get self.send_n_eventranges
      if self.config.has_option(config_section,'send_n_eventranges'):
         self.send_n_eventranges = self.config.getint(config_section,'send_n_eventranges')
      else:
         logger.error('must specify "send_n_eventranges" in "%s" section of config file',config_section)
         return
      logger.info('send_n_eventranges: %d',self.send_n_eventranges)

      # get self.request_n_eventranges
      if self.config.has_option(config_section,'request_n_eventranges'):
         self.request_n_eventranges = self.config.getint(config_section,'request_n_eventranges')
      else:
         logger.error('must specify "request_n_eventranges" in "%s" section of config file',config_section)
         return
      logger.info('request_n_eventranges: %d',self.request_n_eventranges)







