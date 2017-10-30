import os,sys,logging,threading,subprocess,time,multiprocessing
logger = logging.getLogger(__name__)

from pandayoda.common import MessageTypes,serializer,SerialQueue,EventRangeList
from pandayoda.common import MPIService,VariableWithLock,StatefulService

try:
   import yampl
except:
   logger.exception("Failed to import yampl")
   raise

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]

class NoEventRangeDefined(Exception): pass
class NoMoreEventsToProcess(Exception): pass
class FailedToParseYodaMessage(Exception): pass

class JobComm(threading.Thread):
   '''  JobComm: This thread handles all the AthenaMP related payloadcommunication

Pilot/Droid launches AthenaMP and starts listening to its messages. AthenaMP finishes initialization and sends "Ready for events" to Pilot/Droid. Pilot/Droid sends back an event range. AthenaMP sends NWorkers-1 more times "Ready for events" to Pilot/Droid. On each of these messages Pilot/Droid replies with a new event range. After that all AthenaMP workers are busy processing events. Once some AthenaMP worker is available to take the next event range, AthenaMP sends "Ready for events" to Pilot/Droid. Pilot/Droid sends back an event range. Once some output file becomes available, AthenaMP sends full path, RangeID, CPU time and Wall time to Pilot/Droid and does not expect any answer on this message. Here is an example of such message:

"/build1/tsulaia/20.3.7.5/run-es/athenaMP-workers-AtlasG4Tf-sim/worker_1/myHITS.pool.root_000.Range-6,ID:Range-6,CPU:1,WALL:1"

If Pilot/Droid receives "Ready for events"and there are no more ranges to process, it answers with "No more events". This is a signal for AthenaMP that no more events are expected. In this case AthenaMP waits for all its workers to finish processing current event ranges, reports all outputs back to Pilot/Droid and exits.

The event range format is json and is this: [{"eventRangeID": "8848710-3005316503-6391858827-3-10", "LFN":"EVNT.06402143._012906.pool.root.1", "lastEvent": 3, "startEvent": 3, "scope": "mc15_13TeV", "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"}]

   '''

   
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

      # this is used to trigger the thread exit
      self.exit = threading.Event()

   def no_more_work(self):
      return self.all_work_done.get()

   def stop(self):
      ''' this function can be called by outside threads to cause the JobManager thread to exit'''
      self.exit.set()

   def run(self):
      ''' this is the function run as a subthread when the user runs jobComm_instance.start() '''

      self.read_config()

      # setup payloadcommunicator
      payloadcomm = PayloadMessenger(self.yampl_socket_name)
      payloadcomm.start()

      # current panda job that AthenaMP is configured to run
      current_job = None

      # EventRangeList obj
      eventranges = EventRangeList.EventRangeList()

      # this flag is set when it has been determined that the thread can exit
      # after all event ranges have been processed
      no_more_eventranges = False

      # this flag is set when waiting for more event ranges
      NEW_EVENT_RANGES_REQUEST_SENT = False

      # this flag is set when no transform is running
      TRANFORM_IS_RUNNING = False

      # this is the current yampl message from AthenaMP which requires a response
      # if it is set to None, a new message will be requested
      athenamp_msg = None

      while not self.exit.isSet():
         logger.debug('start loop: n-ready: %d n-processing: %d',
            eventranges.number_ready(),eventranges.number_processing())

         # check for messages
         try:
            logger.debug('check for queue message')

            # if there is no current job, then there is nothing to do so block on queue message
            if payloadcomm.get_job_def() is None:
               logger.debug('waiting for job def message')
               qmsg = self.queues['JobComm'].get(block=True,timeout=self.loop_timeout)
            # have a current job, but no events
            elif eventranges.number_ready() == 0:
               # if request has been sent wait for a loop
               if NEW_EVENT_RANGES_REQUEST_SENT:
                  qmsg = self.queues['JobComm'].get(block=True,timeout=self.loop_timeout)
               else:
                  qmsg = self.queues['JobComm'].get(block=False)

            if 'type' not in qmsg:
               logger.error(' no "type" key in the message: %s',qmsg)
               continue
            
            if qmsg['type'] == MessageTypes.NEW_JOB:
               logger.debug('got new job definition from JobManager')
               if 'job' in qmsg:
                  current_job = qmsg['job']
                  # assume transform is now running
                  TRANFORM_IS_RUNNING = True
                  # set the job definition in the PayloadCommunicator
                  payloadcomm.set_job_def(current_job)
               else:
                  logger.error('Received message of NEW_JOB type but no job in message: %s',qmsg)

            elif qmsg['type'] == MessageTypes.NEW_EVENT_RANGES:
               logger.debug('got new event ranges message')

               if 'eventranges' in qmsg:
                  logger.debug('received %d event ranges',len(qmsg['eventranges']))
                  eventranges += EventRangeList.EventRangeList(qmsg['eventranges'])
                  logger.debug('%d eventranges available',len(eventranges))
               else:
                  logger.error('NEW_EVENT_RANGES message type, but no event ranges found.')

               NEW_EVENT_RANGES_REQUEST_SENT = False

            elif qmsg['type'] == MessageTypes.WALLCLOCK_EXPIRING:
               logger.info('received wallclock expiring message')
               # set exit for loop and continue
               self.stop()
               break
            elif qmsg['type'] == MessageTypes.TRANSFORM_EXITED:
               logger.info('received transform exited')
               TRANFORM_IS_RUNNING = False
            else:
               logger.error('Received message of unknown type: %s',qmsg)
         except SerialQueue.Empty:
            logger.debug('no messages on queue')
         
         # if no job definition has been sent from JobManager, there is no reason to try
         # and communicate with AthenaMP since it will not be running
         if payloadcomm.get_job_def() is None:
            continue

         
         # receive a message from AthenaMP if we are not already processing one
         if payloadcomm.is_idle():
            logger.debug('requesting new message from AthenaMP')
            payloadcomm.initiate_request()
         # if payloadcomm reports ready for events, send events
         elif payloadcomm.ready_for_events():
            logger.debug('sending AthenaMP some events')
            
            # get the next event ranges to be processed
            try:
               local_eventranges = eventranges.get_next()
               logger.debug('have %d new event ranges to send to AthenaMP',len(local_eventranges))
            # no more event ranges available
            except EventRangeList.NoMoreEventRanges:
               logger.debug('there are no more event ranges to process')
               # if we have been told there are no more eventranges, then tell the AthenaMP worker there are no more events
               if no_more_eventranges:
                  logger.debug('sending AthenaMP NO_MORE_EVENTS')
                  payloadcomm.send_no_more_events()

               # otherwise continue the loop so more events will be requested
               elif NEW_EVENT_RANGES_REQUEST_SENT:
                  logger.debug('waiting for new event ranges to be received')
               else:
                  logger.debug('sending request for new events')
                  self.request_events(current_job)

                  # set flag so we don't sent the same message again
                  NEW_EVENT_RANGES_REQUEST_SENT = True

            # something wrong with the index in the EventRangeList index
            except EventRangeList.RequestedMoreRangesThanAvailable:
               logger.error('requested more event ranges than available')
               
            else:
               logger.debug('sending eventranges to AthenaMP: %s',local_eventranges)
               # send AthenaMP the new event ranges
               payloadcomm.send_events(local_eventranges)
         
         # if output file is available, send it to Yoda/FileManager 
         elif payloadcomm.has_output_file():
            # Athena sent details of an output file
            logger.debug('received output file from AthenaMP')
            
            # get output file data
            output_file_data = payloadcomm.get_output_file_data()
            # reset payloadcomm for next message
            payloadcomm.reset()

            # send output file data to Yoda/FileManager
            logger.debug('sending output file data to Yoda/FileManager: %s',output_file_data)
            output_file_data['destination_rank'] = 0
            self.queues['MPIService'].put(output_file_data)
            

            # set event range to completed:
            logger.debug('mark event range id %s as completed',output_file_data['eventrangeid'])
            try:
               eventranges.mark_completed(output_file_data['eventrangeid'])
            except:
               logger.error('failed to mark eventrangeid %s as completed',output_file_data['eventrangeid'])
               logger.error('list of assigned: %s',eventranges.indices_of_assigned_ranges)
               logger.error('list of completed: %s',eventranges.indices_of_completed_ranges)
               self.stop()
         else:
            logger.debug('PayloadMessenger has nothing to do')

         # check number of event ranges left, if below some number send new request
         if eventranges.number_ready() < self.get_more_events_threshold and not NEW_EVENT_RANGES_REQUEST_SENT:
            logger.debug('requesting more event ranges')
            self.request_events(current_job)
            NEW_EVENT_RANGES_REQUEST_SENT=True

         # check if all work is done and can exit
         if eventranges.number_processing() == 0 and no_more_eventranges:
            logger.info('All work is complete so initiating exit')
            self.all_work_done.set(True)
            self.stop()
            break

         
         # no other work to do
         if (not NEW_EVENT_RANGES_REQUEST_SENT and
               self.queues['JobComm'].empty() and
               payloadcomm.not_waiting()):
            # so sleep 
            logger.debug('no work on the queues, so sleeping %d',self.loop_timeout)
            time.sleep(self.loop_timeout)
         elif not payloadcomm.not_waiting():
            logger.debug('waiting for eventRangeRetriever to receive data, sleeping')
            time.sleep(self.loop_timeout)
         else:
            logger.debug('continuing loop: %s %s %s %s',current_job is None,self.queues['JobComm'].empty(),not NEW_EVENT_RANGES_REQUEST_SENT,payloadcomm.not_waiting())

      logger.info('sending exit signal to subthreads')
      payloadcomm.stop()
      logger.info('waiting for subthreads to join')
      payloadcomm.join()

      logger.info('JobComm exiting')

   def read_config(self):

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

   def request_events(self,current_job):
      msg={
         'type':MessageTypes.REQUEST_EVENT_RANGES,
         'PandaID':current_job['PandaID'],
         'taskID':current_job['taskID'],
         'jobsetID':current_job['jobsetID'],
         'destination_rank': 0, #YODA rank
      }
      self.queues['MPIService'].put(msg)



class PayloadMessenger(StatefulService.StatefulService):
   ''' This simple thread is run by JobComm to payloadcommunicate with the payload '''


   IDLE                 = 'IDLE'
   REQUEST              = 'REQUEST'
   MESSAGE_RECEIVED     = 'MESSAGE_RECEIVED'
   READY_FOR_EVENTS     = 'READY_FOR_EVENTS'
   OUTPUT_FILE          = 'OUTPUT_FILE'
   SEND_NO_MORE_EVENTS  = 'SEND_NO_MORE_EVENTS'
   SENDING_EVENTS       = 'SENDING_EVENTS'
   EXITED               = 'EXITED'

   # a list of all states
   STATES = [IDLE,REQUEST,MESSAGE_RECEIVED,READY_FOR_EVENTS,OUTPUT_FILE,
             SEND_NO_MORE_EVENTS,SENDING_EVENTS,EXITED]

   # a list of states where the thread is running or busy doing something
   RUNNING_STATES = [REQUEST,SEND_NO_MORE_EVENTS,MESSAGE_RECEIVED,SENDING_EVENTS]

   # a list of states where it is waiting for the controling thread to do something
   # or has exited or is idle.
   NOT_WAITING_FOR_RESPONSE_STATES = [IDLE,REQUEST,MESSAGE_RECEIVED,SENDING_EVENTS,EXITED]

   def __init__(self,yampl_socket_name,loop_timeout = 5):
      super(PayloadMessenger,self).__init__(loop_timeout)

      # yample socket name, must be the same as athena
      self.yampl_socket_name           = yampl_socket_name

      # current job definition
      self.job_def                     = VariableWithLock.VariableWithLock()

      # when in the OUTPUT_FILE state, this variable contains the output file data
      self.output_file_data            = VariableWithLock.VariableWithLock()

      # when in the SENDING_EVENTS state, this variable contains the event range file data
      self.eventranges                 = VariableWithLock.VariableWithLock()



   def reset(self):
      ''' reset the service to the IDLE state '''
      self.set_state(PayloadMessenger.IDLE)

   def set_job_def(self,job):
      ''' set the current job definition '''
      self.job_def.set(job)

   def get_job_def(self):
      ''' retrieve the current job definition '''
      return self.job_def.get()

   def get_output_file_data(self):
      ''' retrieve the output file data, only valid when in state OUTPUT_FILE '''
      return self.output_file_data.get()

   def is_idle(self):
      ''' test if in IDLE state '''
      return self.in_state(PayloadMessenger.IDLE)

   def initiate_request(self):
      ''' initiate a request for a message from the payload '''
      self.set_state(PayloadMessenger.REQUEST)

   def ready_for_events(self):
      ''' test if in READY_FOR_EVENTS state '''
      return self.in_state(PayloadMessenger.READY_FOR_EVENTS)

   def has_output_file(self):
      ''' test if in OUTPUT_FILE state '''
      return self.in_state(PayloadMessenger.OUTPUT_FILE)

   def send_no_more_events(self):
      ''' test if in SEND_NO_MORE_EVENTS state '''
      self.set_state(PayloadMessenger.SEND_NO_MORE_EVENTS)

   def send_events(self,eventranges):
      ''' initiate sending event ranges to payload '''
      self.eventranges.set(eventranges)
      self.set_state(PayloadMessenger.SENDING_EVENTS)

   def not_waiting(self):
      ''' test if in a state that does not require a response from the controlling thread '''
      if self.get_state() in PayloadMessenger.NOT_WAITING_FOR_RESPONSE_STATES:
         return True
      return False


   def run(self):
      ''' service main thread '''


      # set initial state
      self.set_state(self.IDLE)
      
      logger.debug('start yampl payloadcommunicator')
      athpayloadcomm = athena_payloadcommunicator(self.yampl_socket_name)
      payload_msg = ''

      while not self.exit.wait(timeout=self.loop_timeout):
         state = self.get_state()
         logger.debug('start loop, current state: %s',state)

         ##################
         # IDLE: no work to do
         ##################################
         if state == PayloadMessenger.IDLE:
            pass
         
         ##################
         # REQUEST: this state is set by the controling thread and initiates
         #          a request for a message from the payload
         ######################################################################
         elif state == PayloadMessenger.REQUEST:
            logger.debug('checking for message from payload')
            payload_msg = athpayloadcomm.recv()

            if len(payload_msg) > 0:
               logger.debug('received message: %s',payload_msg)
               self.set_state(PayloadMessenger.MESSAGE_RECEIVED)
            else:
               logger.debug('did not receive message from payload')
         
         ##################
         # MESSAGE_RECEIVED: this state indicates that a message has been
         #          received from the payload and its meaning will be parsed
         ######################################################################
         elif state == PayloadMessenger.MESSAGE_RECEIVED:
            if athena_payloadcommunicator.READY_FOR_EVENTS in payload_msg:
               logger.debug('received "%s" message from payload',athena_payloadcommunicator.READY_FOR_EVENTS)
               self.set_state(PayloadMessenger.READY_FOR_EVENTS)
            elif len(payload_msg.split(',')) == 4:
               # Athena sent details of an output file
               logger.debug('received output file from AthenaMP')
               
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
                         }
                  self.output_file_data.set(output_file_data)
                  self.set_state(PayloadMessenger.OUTPUT_FILE)
               else:
                  logger.error('failed to parse output file')
                  self.set_state(PayloadMessenger.IDLE)
            else:
               logger.error('failed to parse message from Athena: %s' % (self.prelog,payload_msg))
               self.set_state(PayloadMessenger.IDLE)
         
         ##################
         # SEND_NO_MORE_EVENTS: this state is set by the controling thread and
         #          and initiates sending a message to the payload that there
         #          are no more events to process
         ######################################################################
         elif state == PayloadMessenger.SEND_NO_MORE_EVENTS:
            logger.debug('sending AthenaMP message that there are no more events')
            athpayloadcomm.send(athena_payloadcommunicator.NO_MORE_EVENTS)
            self.set_state(PayloadMessenger.IDLE)
         
         ##################
         # SENDING_EVENTS: this state is indicates events are being transmitted
         #          to the payload
         ######################################################################
         elif state == PayloadMessenger.SENDING_EVENTS:
            logger.debug('sending AthenaMP more eventranges')
            athpayloadcomm.send(serializer.serialize(self.eventranges.get()))
            self.set_state(PayloadMessenger.IDLE)


      logger.info('PayloadMessenger is exiting')



      

class athena_payloadcommunicator:
   ''' small class to handle yampl payloadcommunication exception handling '''
   READY_FOR_EVENTS        = 'Ready for events'
   NO_MORE_EVENTS          = 'No more events'
   FAILED_PARSE            = 'ERR_ATHENAMP_PARSE'

   def __init__(self,socketname='EventService_EventRanges',context='local'):
      
      # create server socket for yampl
      try:
         self.socket = yampl.ServerSocket(socketname, context)
      except:
         logger.exception('failed to create yampl server socket')
         raise

   def send(self,message):
      # send message using yampl
      try:
         self.socket.send_raw(message)
      except:
         logger.exception("Failed to send yampl message: %s",message)
         raise

   def recv(self):
      # receive yampl message
      size, buf = self.socket.try_recv_raw()
      if size == -1:
         return ''
      return str(buf)











