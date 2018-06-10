import os,logging,threading,time
from pandayoda.common import MessageTypes,SerialQueue,EventRangeList,serializer
from pandayoda.common import MPIService,VariableWithLock,StatefulService

logger = logging.getLogger(__name__)

try:
   import yampl
except Exception,e:
   logger.exception("Failed to import yampl")
   raise

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


class NoEventRangeDefined(Exception):
   pass


class NoMoreEventsToProcess(Exception):
   pass


class FailedToParseYodaMessage(Exception):
   pass


class JobComm(StatefulService.StatefulService):
   '''  JobComm: This thread handles all the AthenaMP related payloadcommunication

Pilot/Droid launches AthenaMP and starts listening to its messages. AthenaMP finishes initialization and sends "Ready for events" to Pilot/Droid. Pilot/Droid sends back an event range. AthenaMP sends NWorkers-1 more times "Ready for events" to Pilot/Droid. On each of these messages Pilot/Droid replies with a new event range. After that all AthenaMP workers are busy processing events. Once some AthenaMP worker is available to take the next event range, AthenaMP sends "Ready for events" to Pilot/Droid. Pilot/Droid sends back an event range. Once some output file becomes available, AthenaMP sends full path, RangeID, CPU time and Wall time to Pilot/Droid and does not expect any answer on this message. Here is an example of such message:

"/build1/tsulaia/20.3.7.5/run-es/athenaMP-workers-AtlasG4Tf-sim/worker_1/myHITS.pool.root_000.Range-6,ID:Range-6,CPU:1,WALL:1"

If Pilot/Droid receives "Ready for events"and there are no more ranges to process, it answers with "No more events". This is a signal for AthenaMP that no more events are expected. In this case AthenaMP waits for all its workers to finish processing current event ranges, reports all outputs back to Pilot/Droid and exits.

The event range format is json and is this: [{"eventRangeID": "8848710-3005316503-6391858827-3-10", "LFN":"EVNT.06402143._012906.pool.root.1", "lastEvent": 3, "startEvent": 3, "scope": "mc15_13TeV", "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"}]

   '''

   WAITING_FOR_JOB            = 'WAITING_FOR_JOB'
   REQUEST_EVENT_RANGES       = 'REQUEST_EVENT_RANGES'
   WAITING_FOR_EVENT_RANGES   = 'WAITING_FOR_EVENT_RANGES'
   MONITORING                 = 'MONITORING'

   STATES = [WAITING_FOR_JOB,WAITING_FOR_EVENT_RANGES,REQUEST_EVENT_RANGES,MONITORING]

   
   def __init__(self,config,queues,droid_working_path,yampl_socket_name):
      '''
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send
                     messages to other Droid components about errors, etc.
        config: the ConfigParser handle for yoda
        droid_working_path: The location of the Droid working area
        '''
      # call base class init function
      super(JobComm,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                      = queues

      # configuration of Yoda
      self.config                      = config

      # working path for droid, where AthenaMP output files will be located
      self.working_path                = droid_working_path

      # socket name to pass to transform for use when communicating via yampl
      self.yampl_socket_name           = yampl_socket_name

      # flag to set when all work is done and thread is exiting
      self.all_work_done               = VariableWithLock.VariableWithLock(False)

      # set initial state
      self.set_state(self.WAITING_FOR_JOB)

   def no_more_work(self):
      return self.all_work_done.get()

   def run(self):
      ''' this is the function run as a subthread when the user runs jobComm_instance.start() '''

      self.read_config()

      # place holder for the payloadcommunicator object
      payloadcomm = None

      # current panda job that AthenaMP is configured to run
      current_job = None

      while not self.exit.isSet():
         logger.debug('start loop: state: %s',self.get_state())

         ##################
         # WAITING_FOR_JOB: waiting for the job definition to arrive, before
         #        it does, it is assumed that there is no payload running
         ######################################################################
         if self.get_state() == self.WAITING_FOR_JOB:
            logger.debug(' waiting for job definition, blocking on message queue for %s ',self.loop_timeout)
            try:
               qmsg = self.queues['JobComm'].get(block=True,timeout=self.loop_timeout)
            except SerialQueue.Empty:
               logger.debug('no message on queue')
            else:
               # shorten our message for printing
               if logger.getEffectiveLevel() == logging.DEBUG:
                  tmpmsg = str(qmsg)
                  if len(tmpmsg) > 100:
                     tmpslice = slice(0,100)
                     tmpmsg = tmpmsg[tmpslice] + '...'
                  logger.debug('received queue message: %s',tmpmsg)
               if 'type' not in qmsg or qmsg['type'] != MessageTypes.NEW_JOB or 'job' not in qmsg:
                  logger.error('received unexpected message format: %s',qmsg)
               else:
                  logger.debug('received job definition')
                  current_job = qmsg['job']
                  
                  # create new payload communicator
                  payloadcomm = PayloadMessenger(self.yampl_socket_name,
                                                 self.queues,
                                                 self.loop_timeout,
                                                 self.aggregate_output_files_time)
                  payloadcomm.start()

                  # set the job definition in the PayloadCommunicator
                  payloadcomm.set_job_def(current_job)

                  # change state
                  self.set_state(self.REQUEST_EVENT_RANGES)
            qmsg = None

         ##################
         # REQUEST_EVENT_RANGES: Request event ranges from Yoda
         ######################################################################
         elif self.get_state() == self.REQUEST_EVENT_RANGES:
            logger.debug('requesting event ranges')
            if payloadcomm.no_more_events():
               logger.debug('no more event ranges flag is set, going to MONITORING state')
               self.set_state(self.MONITORING)
            else:
               # send MPI message to Yoda for more event ranges
               self.request_events(current_job)
               # change state
               self.set_state(self.WAITING_FOR_EVENT_RANGES)
         
         ##################
         # WAITING_FOR_EVENT_RANGES: Waiting for event ranges from Yoda
         ######################################################################
         elif self.get_state() == self.WAITING_FOR_EVENT_RANGES:
            logger.debug('waiting for event ranges, blocking on message queue for %s',self.loop_timeout)
            try:
               qmsg = self.queues['JobComm'].get(block=True,timeout=self.loop_timeout)
            except SerialQueue.Empty:
               logger.debug('no message on queue')
            else:
               # shorten our message for printing
               if logger.getEffectiveLevel() == logging.DEBUG:
                  tmpmsg = str(qmsg)
                  if len(tmpmsg) > 100:
                     tmpslice = slice(0,100)
                     tmpmsg = tmpmsg[tmpslice] + '...'
                  logger.debug('received queue message: %s',tmpmsg)
               if 'type' not in qmsg:
                  logger.error('received unexpected message format: %s',qmsg)
               elif qmsg['type'] == MessageTypes.NEW_EVENT_RANGES:
                  logger.debug('received event ranges')
                  eventranges = EventRangeList.EventRangeList(qmsg['eventranges'])
                  # add event ranges to payload messenger list
                  payloadcomm.add_eventranges(eventranges)
                  # change state
                  self.set_state(self.MONITORING)
               elif qmsg['type'] == MessageTypes.NO_MORE_EVENT_RANGES:
                  logger.debug('no more event ranges for PandaID %s going to MONITORING state',qmsg['PandaID'])
                  payloadcomm.set_no_more_events()
                  # if there are no more event ranges, tell payload to stop
                  if payloadcomm.get_number_eventranges() - payloadcomm.get_ready_eventranges() == 0:
                     payloadcomm.stop()
                     self.stop()
                     self.all_work_done.set(True)
                  else:
                     self.set_state(self.MONITORING)
               else:
                  logger.error('unknown message type: %s',qmsg['type'])


            qmsg = None
         elif self.get_state() == self.MONITORING:
            logger.debug('monitoring payload')

            ## check if running low on event ranges
            ready_events = payloadcomm.get_ready_eventranges()
            number_completed = payloadcomm.get_completed_eventranges()
            total = payloadcomm.get_number_eventranges()
            logger.debug('number of ready events %s; number of completed events %s; total events %s',ready_events,number_completed,total)
               
            # if ready_events is below the threshold and the no more events flag has not been set
            # request more event ranges
            if ready_events < self.get_more_events_threshold and not payloadcomm.no_more_events():
               logger.debug('number of ready events %s below request threshold %s, asking for more.',ready_events,self.get_more_events_threshold)
               # change state to request event ranges
               self.set_state(self.REQUEST_EVENT_RANGES)
            
            # if the number of completed events equals the number of event ranges
            # available, and no more events flag is set, then kill subprocess and exit.
            elif ready_events == 0 and number_completed == total and payloadcomm.no_more_events():
               logger.debug('no more events to process, exiting')
               payloadcomm.stop()
               self.stop()
               self.all_work_done.set(True)
            else:
               logger.debug('sleeping for %s',self.loop_timeout)
               self.exit.wait(timeout=self.loop_timeout)


      logger.info('sending exit signal to subthreads')
      payloadcomm.stop()
      logger.info('waiting for subthreads to join')
      payloadcomm.join()
      

      logger.info('JobComm exiting')

   def read_config(self):

      # read  log level:
      if self.config.has_option(config_section,'loglevel'):
         self.loglevel = self.config.get(config_section,'loglevel')
         logger.info('%s loglevel: %s',config_section,self.loglevel)
         logger.setLevel(logging.getLevelName(self.loglevel))
      else:
         logger.warning('no "loglevel" in "%s" section of config file, keeping default',config_section)

      # get loop_timeout
      if self.config.has_option(config_section,'loop_timeout'):
         self.loop_timeout = self.config.getfloat(config_section,'loop_timeout')
      else:
         logger.error('must specify "loop_timeout" in "%s" section of config file',config_section)
         return
      if MPIService.rank == 0:
         logger.info('JobComm loop_timeout: %d',self.loop_timeout)

      # get get_more_events_threshold
      if self.config.has_option(config_section,'get_more_events_threshold'):
         self.get_more_events_threshold = self.config.getint(config_section,'get_more_events_threshold')
      else:
         logger.error('must specify "get_more_events_threshold" in "%s" section of config file',config_section)
         return
      if MPIService.rank == 0:
         logger.info('JobComm get_more_events_threshold: %d',self.get_more_events_threshold)


      # get aggregate_output_files_time
      if self.config.has_option(config_section,'aggregate_output_files_time'):
         self.aggregate_output_files_time = self.config.getint(config_section,'aggregate_output_files_time')
      else:
         logger.error('must specify "aggregate_output_files_time" in "%s" section of config file',config_section)
         return
      if MPIService.rank == 0:
         logger.info('JobComm get_more_events_threshold: %d',self.aggregate_output_files_time)

   def request_events(self,current_job):
      msg = {
            'type':MessageTypes.REQUEST_EVENT_RANGES,
            'PandaID':current_job['PandaID'],
            'taskID':current_job['taskID'],
            'jobsetID':current_job['jobsetID'],
            'destination_rank': 0,  # YODA rank
      }
      self.queues['MPIService'].put(msg)



class PayloadMessenger(StatefulService.StatefulService):
   ''' This simple thread is run by JobComm to payloadcommunicate with the payload '''

   WAIT_FOR_PAYLOAD_MESSAGE      = 'WAIT_FOR_PAYLOAD_MESSAGE'
   MESSAGE_RECEIVED              = 'MESSAGE_RECEIVED'
   WAITING_FOR_EVENT_RANGES      = 'WAITING_FOR_EVENT_RANGES'
   SEND_EVENT_RANGE              = 'SEND_EVENT_RANGE'
   SEND_OUTPUT_FILE              = 'SEND_OUTPUT_FILE'


   # a list of all states
   STATES = [WAIT_FOR_PAYLOAD_MESSAGE,MESSAGE_RECEIVED,WAITING_FOR_EVENT_RANGES,
             SEND_EVENT_RANGE,SEND_OUTPUT_FILE]


   def __init__(self,yampl_socket_name,queues,loop_timeout=1,aggregate_output_files_time=0):
      super(PayloadMessenger,self).__init__(loop_timeout)

      # yampl socket name, must be the same as athena
      self.yampl_socket_name           = yampl_socket_name

      # queues with which to communicate with JobComm and MPIService
      self.queues                      = queues

      # time over which to aggregate output file data before communicating it to Yoda
      self.aggregate_output_files_time = aggregate_output_files_time

      # current job definition
      self.job_def                     = VariableWithLock.VariableWithLock()

      # when in the SENDING_EVENTS state, this variable contains the event range file data
      self.eventranges                 = VariableWithLock.VariableWithLock(EventRangeList.EventRangeList())

      # set by JobComm when no more events ready
      self.no_more_eventranges         = threading.Event()

      # add class prelog to log output to make it clear where messages are coming from
      self.prelog                      = self.__class__.__name__

      # set initial state
      self.set_state(PayloadMessenger.WAIT_FOR_PAYLOAD_MESSAGE)

   def get_ready_eventranges(self):
      return self.eventranges.get().number_ready()
   
   def get_completed_eventranges(self):
      return self.eventranges.get().number_completed()
   
   def get_number_eventranges(self):
      return len(self.eventranges.get())

   def add_eventranges(self,eventranges):
      # acquire lock
      self.eventranges.lock.acquire()
      # add the event ranges
      self.eventranges.variable += eventranges
      # release lock
      self.eventranges.lock.release()
      self.eventranges.is_set_flag.set()

   def get_next_eventrange(self):

      # acquire lock
      self.eventranges.lock.acquire()
      # add the event ranges
      try:
         next = self.eventranges.variable.get_next()
      except Exception:
         # release lock
         self.eventranges.lock.release()
         raise

      # release lock
      self.eventranges.lock.release()

      return next

   def mark_completed_eventrange(self,eventrangeID):
      # acquire lock
      self.eventranges.lock.acquire()
      # add the event ranges
      self.eventranges.variable.mark_completed(eventrangeID)
      # release lock
      self.eventranges.lock.release()


   def set_job_def(self,job):
      ''' set the current job definition '''
      self.job_def.set(job)

   def get_job_def(self):
      ''' retrieve the current job definition '''
      return self.job_def.get()

   def set_no_more_events(self):
      ''' test if no_more_eventranges set '''
      self.no_more_eventranges.set()
   
   def no_more_events(self):
      ''' return value of flag '''
      return self.no_more_eventranges.isSet()



   def run(self):
      ''' service main thread '''

      logger.debug('%s: start yampl payloadcommunicator',self.prelog)
      athpayloadcomm = athena_payloadcommunicator(self.yampl_socket_name)
      payload_msg = ''

      # current list of output files to send via MPI
      output_files = []
      last_output_file_mpi_send = time.time()

      event_range_request_counter = 0

      while not self.exit.isSet():
         logger.debug('%s: start loop, current state: %s',self.prelog,self.get_state())

         # don't want to hammer Yoda with lots of little messages for output files
         # so aggregate output files for some time period then send as a group
         if len(output_files) == 0:
            last_output_file_mpi_send = time.time()
         elif (time.time() - last_output_file_mpi_send) > self.aggregate_output_files_time:

               # send output file data to Yoda/FileManager
               logger.debug('%s: sending %s output files to Yoda/FileManager',self.prelog,len(output_files))
               mpi_message = {'type':MessageTypes.OUTPUT_FILE,
                              'filelist':output_files,
                              'destination_rank': 0
                             }
               self.queues['MPIService'].put(mpi_message)

               # set time for next send
               last_output_file_mpi_send = time.time()
               # reset output file list
               output_files = []

         
         ##################
         # WAIT_FOR_PAYLOAD_MESSAGE: initiates
         #          a request for a message from the payload
         ######################################################################
         if self.get_state() == self.WAIT_FOR_PAYLOAD_MESSAGE:
            
            # if requests for events have been sent and we are waiting for event ranges
            # do not block
            temp_timeout = self.loop_timeout
            if event_range_request_counter > 0:
               temp_timeout = 1
            
            logger.debug('%s: checking for message from payload, block for %s, event range requests: %s',self.prelog,self.loop_timeout,event_range_request_counter)

            payload_msg = athpayloadcomm.recv(temp_timeout)

            if len(payload_msg) > 0:
               logger.debug('%s: received message: %s',self.prelog,payload_msg)
               self.set_state(self.MESSAGE_RECEIVED)
            else:
               logger.debug('%s: did not receive message from payload',self.prelog)
               if event_range_request_counter > 0:
                  logger.debug('%s: have %s pending event range requests so will try sending one.',self.prelog,event_range_request_counter)
                  self.set_state(self.SEND_EVENT_RANGE)
               # time.sleep(self.loop_timeout)
         
         ##################
         # MESSAGE_RECEIVED: this state indicates that a message has been
         #          received from the payload and its meaning will be parsed
         ######################################################################
         elif self.get_state() == self.MESSAGE_RECEIVED:
            
            # if ready for events, send them or wait for some
            if athena_payloadcommunicator.READY_FOR_EVENTS in payload_msg:
               logger.info('%s: received "%s" from payload',self.prelog,payload_msg)
               self.set_state(self.SEND_EVENT_RANGE)
               # increment counter to keep track of how many requests are queued
               event_range_request_counter += 1


            #### OUTPUT File received
            elif len(payload_msg.split(',')) == 4:
               # Athena sent details of an output file
               logger.info('%s: received output file from AthenaMP',self.prelog)
               
               self.set_state(self.SEND_OUTPUT_FILE)

            else:
               logger.error('%s: failed to parse message from Athena: %s' % (self.prelog,payload_msg))
               self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)
         
         ##################
         # SEND_EVENT_RANGE: wait until more event ranges are sent by JobComm
         ######################################################################
         elif self.get_state() == self.SEND_EVENT_RANGE:
            logger.debug('%s: sending event to payload',self.prelog)
            # if event ranges available, send one
            try:
               local_eventranges = self.get_next_eventrange()
               logger.debug('%s: have %d ready event ranges to send to AthenaMP',self.prelog,self.eventranges.get().number_ready())
            # no more event ranges available
            except EventRangeList.NoMoreEventRanges:
               logger.debug('%s: there are no more event ranges to process',self.prelog)
               # if we have been told there are no more eventranges, then tell the AthenaMP worker there are no more events
               if self.no_more_eventranges.isSet():
                  logger.info('%s: sending AthenaMP NO_MORE_EVENTS',self.prelog)
                  athpayloadcomm.send(athena_payloadcommunicator.NO_MORE_EVENTS)

               # otherwise wait for more events
               else:
                  logger.info('%s: waiting for more events for',self.prelog)
               

               # return to state requesting a message
               self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)



            # something wrong with the index in the EventRangeList index
            except EventRangeList.RequestedMoreRangesThanAvailable:
               logger.error('%s: requested more event ranges than available, going to try waiting for more events',self.prelog)
               if self.eventranges.wait(self.loop_timeout):
                  logger.debug('%s: eventranges set, check again',self.prelog)
                  self.eventranges.clear()
               else:
                  logger.debug('%s: eventranges not yet set, looping again',self.prelog)
            else:
               logger.info('%s: sending eventranges to AthenaMP: %s',self.prelog,local_eventranges)
               # append full path to file name for AthenaMP
               # and adjust event counter by the number of files
               # input_files = self.job_def.get()['inFiles'].split(',')
               # logger.debug('%s: found %s input files',self.prelog,len(input_files))
               for evtrg in local_eventranges:
                  evtrg['PFN'] = os.path.join(os.getcwd(),evtrg['LFN'])
                  '''
                  # get index of the file in the input file list
                  # counting from 0
                  filenum = input_files.index(evtrg['LFN'])
                  logger.debug('%s: file %s is index %s',self.prelog,evtrg['LFN'],filenum)
                  for i in range(filenum+1):
                     logger.debug('%s: file %s = %s',self.prelog,i,input_files[i])
                  logger.debug('%s: startEvent = %s',self.prelog,evtrg['startEvent'])
                  evtrg['startEvent'] = evtrg['startEvent'] - filenum*500
                  logger.debug('%s: startEvent = %s',self.prelog,evtrg['startEvent'])
                  evtrg['lastEvent'] = evtrg['lastEvent'] - filenum*500'''


               


               # send AthenaMP the new event ranges
               athpayloadcomm.send(serializer.serialize(local_eventranges))
               # decrement counter since we sent some events
               event_range_request_counter -= 1

               # return to state requesting a message
               self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)

         ##################
         # WAITING_FOR_EVENT_RANGES: out of event ranges, wait for more to be set
         ######################################################################
         elif self.get_state() == self.WAITING_FOR_EVENT_RANGES:
            logger.debug('%s: waiting for more event ranges for %s',self.prelog,self.loop_timeout)
            if self.eventranges.wait(self.loop_timeout):
               logger.debug('%s: eventranges set, check again',self.prelog)
               self.eventranges.clear()
               self.set_state(self.SEND_EVENT_RANGE)
            else:
               logger.debug('%s: eventranges not yet set, incrementing ',self.prelog)
               


         ##################
         # SEND_OUTPUT_FILE: send output file data to MPIService
         ######################################################################
         elif self.get_state() == self.SEND_OUTPUT_FILE:
            logger.debug('%s: send output file information',self.prelog)
            
            # parse message
            parts = payload_msg.split(',')
            # there should be four parts:
            # "myHITS.pool.root_000.Range-6,ID:Range-6,CPU:1,WALL:1"
            if len(parts) == 4:
               # parse the parts
               outputfilename = parts[0]
               eventrangeid = parts[1].replace('ID:','')
               cpu = parts[2].replace('CPU:','')
               wallclock = parts[3].replace('WALL:','')
               output_file_data = {'type':MessageTypes.OUTPUT_FILE,
                                   'filename':outputfilename,
                                   'eventrangeid':eventrangeid,
                                   'cpu':cpu,
                                   'wallclock':wallclock,
                                   'scope':self.get_job_def()['scopeOut'],
                                   'pandaid':self.job_def.get()['PandaID'],
                                   'eventstatus':'finished',
                                   'destination_rank': 0,
                                  }
               # self.output_file_data.set(output_file_data)

               # append output file data to list of files for transfer via MPI
               output_files.append(output_file_data)
               
               # return to state requesting a message
               self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)

               # set event range to completed:
               logger.debug('%s: mark event range id %s as completed',self.prelog,output_file_data['eventrangeid'])
               try:
                  self.mark_completed_eventrange(output_file_data['eventrangeid'])
               except Exception:
                  logger.error('failed to mark eventrangeid %s as completed',output_file_data['eventrangeid'])
                  logger.error('list of assigned: %s',self.eventranges.get().indices_of_assigned_ranges)
                  logger.error('list of completed: %s',self.eventranges.get().indices_of_completed_ranges)
                  self.stop()
               
            else:
               logger.error('%s: failed to parse output file',self.prelog)
               self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)

            
         else:
            logger.error('state not recognized')


      logger.info('%s: PayloadMessenger is exiting',self.prelog)



      

class athena_payloadcommunicator:
   ''' small class to handle yampl payloadcommunication exception handling '''
   READY_FOR_EVENTS        = 'Ready for events'
   NO_MORE_EVENTS          = 'No more events'
   FAILED_PARSE            = 'ERR_ATHENAMP_PARSE'

   def __init__(self,socketname='EventService_EventRanges',context='local'):
      
      # create server socket for yampl
      try:
         self.socket = yampl.ServerSocket(socketname, context)
      except Exception:
         logger.exception('failed to create yampl server socket')
         raise

   def send(self,message):
      # send message using yampl
      try:
         self.socket.send_raw(message)
      except Exception:
         logger.exception("Failed to send yampl message: %s",message)
         raise

   def recv(self,timeout=0):
      # receive yampl message, timeout is in milliseconds
      size, buf = self.socket.try_recv_raw(timeout * 1000)
      if size == -1:
         return ''
      return str(buf)

