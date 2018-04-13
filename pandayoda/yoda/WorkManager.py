import os,threading,logging,time
import RequestHarvesterJob,RequestHarvesterEventRanges,PandaJobDict
from pandayoda.common import MessageTypes,SerialQueue,EventRangeList
from pandayoda.common import MPIService
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

      # pending requests from droid ranks
      self.pending_requests      = []
      # helps track which request I am processing
      self.pending_index         = 0

      # start a Request Havester Job thread to begin getting a job
      requestHarvesterJob        = RequestHarvesterJob.RequestHarvesterJob(self.config,self.queues)
      requestHarvesterJob.start()

      # place holder for Request Harevester Event Ranges instance (wait for job definition before launching)
      requestHarvesterEventRanges = None

      # set MPIService to block on incoming MPI traffic
      MPIService.mpiService.set_mpi_blocking()


      while not self.exit.isSet():
         logger.debug('start loop')


         ################
         # check for queue messages
         ################################
         qmsg = None
         if not self.queues['WorkManager'].empty():
            logger.debug('queue has messages')
            qmsg = self.queues['WorkManager'].get(block=False)
            # any time I get a message from this queue, I reset the index of the pending request list
            # this way, I cycle through the pending requests once per new message
            self.pending_index = 0
         # if main queue is empty, process pending message queues
         elif len(self.pending_requests) > 0:
            if self.pending_index < len(self.pending_requests):
               logger.debug('pending queue has %s messages processing %s',len(self.pending_requests),self.pending_index)
               qmsg = self.pending_requests[self.pending_index]
            else:
               logger.debug('have cycled through all pending requests without a change, will block on queue for %s',self.loop_timeout)
               try:
                  self.pending_index = 0
                  qmsg = self.queues['WorkManager'].get(block=True,timeout=self.loop_timeout)
               except SerialQueue.Empty:
                  logger.debug('no messages on queue after blocking')
         elif not (
              (requestHarvesterJob is not None and requestHarvesterJob.jobs_ready()) and
              (requestHarvesterEventRanges is not None and requestHarvesterEventRanges.eventranges_ready())
                  ):
            try:
               self.pending_index = 0
               qmsg = self.queues['WorkManager'].get(block=True,timeout=self.loop_timeout)
            except SerialQueue.Empty:
               logger.debug('no messages on queue after blocking')
         
         if qmsg:
            logger.debug('received message %s',qmsg)


            #############
            ## Just a message to cause WorkManager to wake from sleep and process
            ###############################
            if qmsg['type'] == MessageTypes.WAKE_UP:
               continue

            #############
            ## DROID requesting new job
            ###############################
            elif qmsg['type'] == MessageTypes.REQUEST_JOB:
               logger.debug('droid requesting job description')

               # Do I have a panda job to give out?
               # if not, create a new request if no request is active
               if len(pandajobs) == 0:
                  # if there are no panda jobs and requestHarvesterJob is None, then start a request
                  # this really should never happen.
                  logger.debug('There are no panda jobs')
                  if requestHarvesterJob is None:
                     logger.info('launching new job request')
                     requestHarvesterJob = RequestHarvesterJob.RequestHarvesterJob(self.config,self.queues)
                     requestHarvesterJob.start()
                  else:
                     if requestHarvesterJob.running():
                        logger.debug('request is running, adding message to pending and will process again later')
                     elif requestHarvesterJob.exited():
                        logger.debug('request has exited')
                        jobs = requestHarvesterJob.get_jobs()
                        if jobs is None:
                           logger.error('request has exited and returned no events, reseting request object')
                           if requestHarvesterJob.is_alive():
                              requestHarvesterJob.stop()
                              logger.info('waiting for requestHarvesterJob to join')
                              requestHarvesterJob.join()
                           requestHarvesterJob = None
                        else:
                           logger.debug('new jobs ready, adding to PandaJobDict, then add to pending requests')
                           pandajobs.append_from_dict(jobs)
                           # add to pending requests because the job to send will be chose by another
                           # section of code below which sends the job based on the event ranges on hand

                           # reset job request
                           requestHarvesterJob = None
                           # backup pending counter
                           self.pending_index += -1

                     else:
                        if requestHarvesterJob.state_lifetime() > 60:
                           logger.error('request is stuck in state %s recreating it.',requestHarvesterJob.get_state())
                           if requestHarvesterJob.is_alive():
                              requestHarvesterJob.stop()
                              logger.info('waiting for requestHarvesterJob to join')
                              requestHarvesterJob.join()
                           requestHarvesterJob = None
                        else:
                           logger.debug('request is in %s state, waiting',requestHarvesterJob.get_state())
                  
                  logger.info('pending message')
                  # place request on pending_requests queue and reprocess again when job is ready
                  self.pend_request(qmsg)

               
               # There are jobs in the list so choose one to send
               # The choice depends on numbers of events available for each job
               elif len(pandajobs) == 1:
                  logger.debug('There is one job so send it.')
                  
                  # get the job
                  pandaid = pandajobs.keys()[0]
                  job = pandajobs[pandaid]

                  # send it to droid rank
                  logger.debug('sending droid rank %s panda id %s which has the most ready events %s',
                               qmsg['source_rank'],pandaid,job.number_ready())
                  outmsg = {
                     'type':MessageTypes.NEW_JOB,
                     'job':job.job_def,
                     'destination_rank':qmsg['source_rank']
                  }
                  self.queues['MPIService'].put(outmsg)
                     
                  if qmsg in self.pending_requests:
                     self.pending_requests.remove(qmsg)
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
               droid_pandaid = str(qmsg['PandaID'])

               # if there are no event ranges left reply with such
               if pandajobs[droid_pandaid].eventranges.no_more_event_ranges:
                  logger.debug('no event ranges left for panda ID %s',droid_pandaid)
                  logger.debug('sending NO_MORE_EVENT_RANGES to rank %s',qmsg['source_rank'])
                  self.queues['MPIService'].put(
                        {'type':MessageTypes.NO_MORE_EVENT_RANGES,
                         'destination_rank':qmsg['source_rank'],
                         'PandaID':droid_pandaid,
                        })

                  # remove message if from pending
                  if qmsg in self.pending_requests:
                     self.pending_requests.remove(qmsg)
                  qmsg = None

               # may still be event ranges for this ID
               else:
                  logger.debug('retrieving event ranges for panda ID %s',droid_pandaid)

                  # event ranges found for pandaID
                  if str(droid_pandaid) in pandajobs.keys():
                     logger.debug('EventRangeList object for pandaID %s exists, events ready %s',droid_pandaid,pandajobs[droid_pandaid].number_ready())
                     
                     # have event ranges ready
                     if pandajobs[droid_pandaid].number_ready() > 0:
                        self.send_eventranges(pandajobs[droid_pandaid].eventranges,qmsg)

                        # remove message if from pending
                        if qmsg in self.pending_requests:
                           self.pending_requests.remove(qmsg)
                        qmsg = None

                     # no event ranges remaining, will request more
                     else:
                        logger.debug('no eventranges remain for pandaID %s, can we request more?',droid_pandaid)
                        
                        # check if the no more event ranges flag has been set for this panda id
                        if pandajobs[droid_pandaid].eventranges.no_more_event_ranges:
                           # set the flag so we know there are no more events for this PandaID
                           logger.debug('no more event ranges for PandaID: %s',droid_pandaid)
                           logger.debug('sending NO_MORE_EVENT_RANGES to rank %s',qmsg['source_rank'])
                           self.queues['MPIService'].put(
                                 {'type':MessageTypes.NO_MORE_EVENT_RANGES,
                                  'destination_rank':qmsg['source_rank'],
                                  'PandaID':droid_pandaid,
                                 })

                           # remove message if from pending
                           if qmsg in self.pending_requests:
                              self.pending_requests.remove(qmsg)
                           qmsg = None

                        # flag is not set, so if no request is running create one
                        elif requestHarvesterEventRanges is None:
                           logger.debug('requestHarvesterEventRanges does not exist, creating new request')
                           requestHarvesterEventRanges = RequestHarvesterEventRanges.RequestHarvesterEventRanges(
                                 self.config,
                                 {
                                  'pandaID':pandajobs[droid_pandaid]['PandaID'],
                                  'jobsetID':pandajobs[droid_pandaid]['jobsetID'],
                                  'taskID':pandajobs[droid_pandaid]['taskID'],
                                  'nRanges':self.request_n_eventranges,
                                 }
                              )
                           requestHarvesterEventRanges.start()

                           # pend the request for later processing
                           self.pend_request(qmsg)
                        # there is a request, if it is running, pend the message for later processing
                        elif requestHarvesterEventRanges.running():
                           logger.debug('requestHarvesterEventRanges is running, will pend this request and check again')
                           self.pend_request(qmsg)
                        # there is a request, if it is has exited, check if there are events available
                        elif requestHarvesterEventRanges.exited():
                           logger.debug('requestHarvesterEventRanges exited, will check for new event ranges')

                           # if no more events flag is set, there are no more events for this PandaID
                           if requestHarvesterEventRanges.no_more_eventranges():
                              # set the flag so we know there are no more events for this PandaID
                              logger.debug('no more event ranges for PandaID: %s',droid_pandaid)
                              pandajobs[droid_pandaid].eventranges.no_more_event_ranges = True
                              logger.debug('sending NO_MORE_EVENT_RANGES to rank %s',qmsg['source_rank'])
                              self.queues['MPIService'].put(
                                    {'type':MessageTypes.NO_MORE_EVENT_RANGES,
                                     'destination_rank':qmsg['source_rank'],
                                     'PandaID':droid_pandaid,
                                    })

                              # remove message if from pending
                              if qmsg in self.pending_requests:
                                 self.pending_requests.remove(qmsg)
                              qmsg = None

                           # event ranges received so add them to the list
                           else:
                              tmpeventranges = requestHarvesterEventRanges.get_eventranges()

                              if tmpeventranges is not None:
                                 logger.debug('received eventranges: %s',
                                    ' '.join(('%s:%i' % (tmpid,len(tmplist))) for tmpid,tmplist in tmpeventranges.iteritems()))
                                 # add event ranges to pandajobs dict
                                 for jobid,ers in tmpeventranges.iteritems():
                                    pandajobs[jobid].eventranges += EventRangeList.EventRangeList(ers)
                                    # events will be sent in the next loop execution

                                 self.send_eventranges(pandajobs[jobid].eventranges,qmsg)

                                 # remove message if from pending
                                 if qmsg in self.pending_requests:
                                    self.pending_requests.remove(qmsg)
                                 qmsg = None
                              else:
                                 logger.error('no eventranges after requestHarvesterEventRanges exited, starting new request')
                                 self.pend_request(qmsg)
                           
                           # reset request
                           requestHarvesterEventRanges = None

                        else:
                           logger.error('requestHarvesterEventRanges is in strange state %s, restarting',requestHarvesterEventRanges.get_state())
                           requestHarvesterEventRanges = None
                           self.pend_request(qmsg)
 
                  # something went wrong
                  else:
                     logger.error('there is no eventrange for pandaID %s, this should be impossible since every pandaID in the pandajobs dictionary gets an empty EventRangeList object. Something is amiss. panda job ids: %s',droid_pandaid,pandajobs.keys())

            else:
               logger.error('message type was not recognized: %s',qmsg['type'])
         
         
         

         # if there is nothing to be done, sleep

         if (requestHarvesterJob is not None and requestHarvesterJob.running()) and \
            (requestHarvesterEventRanges is not None and requestHarvesterEventRanges.running()) and \
            self.queues['WorkManager'].empty() and self.pending_requests.empty():
            time.sleep(self.loop_timeout)
         else:
            logger.debug('continuing loop')
            if requestHarvesterJob is not None:
               logger.debug('RequestHarvesterJob: %s',requestHarvesterJob.get_state())
            if requestHarvesterEventRanges is not None:
               logger.debug('requestHarvesterEventRanges: %s',requestHarvesterEventRanges.get_state())
            

      logger.info('signaling exit to threads')
      if requestHarvesterJob is not None and requestHarvesterJob.is_alive():
         logger.debug('signaling requestHarvesterJob to stop')
         requestHarvesterJob.stop()
      if requestHarvesterEventRanges is not None and requestHarvesterEventRanges.is_alive():
         logger.debug('signaling requestHarvesterEventRanges to stop')
         requestHarvesterEventRanges.stop()

      if requestHarvesterJob is not None and requestHarvesterJob.is_alive():
         logger.debug('waiting for requestHarvesterJob to join')
         requestHarvesterJob.join()
      if requestHarvesterEventRanges is not None and requestHarvesterEventRanges.is_alive():
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

   def send_eventranges(self,eventranges,qmsg):
      ''' send the requesting MPI rank some event ranges '''
      # get a  subset of ready eventranges up to the send_n_eventranges value
      local_eventranges = eventranges.get_next(min(eventranges.number_ready(),self.send_n_eventranges))
      
      # send event ranges to Droid
      logger.debug('sending %d new event ranges to droid rank %d',len(local_eventranges),qmsg['source_rank'])
      outmsg = {
         'type':MessageTypes.NEW_EVENT_RANGES,
         'eventranges': local_eventranges,
         'destination_rank':qmsg['source_rank'],
      }
      self.queues['MPIService'].put(outmsg)



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

   def pend_request(self,msg):
      if msg in self.pending_requests:
         self.pending_index += 1
      else:
         self.pending_requests.append(msg)
         self.pending_index = 0

