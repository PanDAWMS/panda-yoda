import os,sys,logging,threading,subprocess,time,multiprocessing
logger = logging.getLogger(__name__)

from pandayoda.payloadcommon import MessageTypes,serializer,SerialQueue,EventRangeList
from pandayoda.payloadcommon import yoda_droid_messenger as ydm,VariableWithLock,StatefulService

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

   
   def __init__(self,config,queues,droid_working_path):
      ''' 
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send 
                     messages to other Droid components about errors, etc.
        config: the ConfigParser handle for yoda
        '''
      # call base class init function
      super(JobComm,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                      = queues

      # configuration of Yoda
      self.config                      = config

      # working path for droid, where AthenaMP output files will be located
      self.working_path                = droid_working_path


      # flag to set when all work is done and thread is exiting
      self.all_work_done               = VariableWithLock.VariableWithLock(False)

      # this is used to trigger the thread exit
      self.exit = threading.Event()

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

      # this is the current yampl message from AthenaMP which requires a response
      # if it is set to None, a new message will be requested
      athenamp_msg = None

      while not self.exit.isSet():
         logger.debug('start loop: n-ready: %d n-processing: %d',
            eventranges.number_ready(),eventranges.number_processing())

         # check for messages
         try:
            logger.debug('check for job definition from JobManager')
            qmsg = self.queues['JobComm'].get(block=False)
            
            if qmsg['type'] == MessageTypes.NEW_JOB:
               logger.debug('got new job definition from JobManager')
               if 'job' in qmsg:
                  current_job = qmsg['job']
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

            elif qmsg['type'] == MessageTypes.WALLCLOCK_EXPIRING:
               logger.info('received wallclock expiring message')
               # set exit for loop and continue
               self.stop()
               break
            else:
               logger.error('Received message of unknown type: %s',qmsg)
         except SerialQueue.Empty:
            logger.debug('no jobs from JobManager')


         
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
                  msg={
                     'type':MessageTypes.REQUEST_EVENT_RANGES,
                     'PandaID':current_job['PandaID'],
                     'taskID':current_job['taskID'],
                     'jobsetID':current_job['jobsetID'],
                     'destination_rank': 0, #YODA rank
                  }
                  self.queues['MPIService'].put(msg)

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


         # check if all work is done and can exit
         if eventranges.number_processing() == 0 and no_more_eventranges:
            logger.info('All work is complete so initiating exit')
            self.all_work_done.set(True)
            self.stop()
            break

         # should sleep if JobManager has yet to send a job
         if current_job is None and self.queues['JobComm'].empty():
            logger.debug('no job and none queued, so sleeping %d',loop_timeout)
            time.sleep(self.loop_timeout)
         # no other work to do
         elif (not NEW_EVENT_RANGES_REQUEST_SENT and
               self.queues['JobComm'].empty() and
               payloadcomm.not_waiting()):
            # so sleep 
            logger.debug('no work on the queues, so sleeping %d',loop_timeout)
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
      if self.rank == 0:
         logger.info('JobComm loop_timeout: %d',self.loop_timeout)

      # get yampl_socket_name
      if self.config.has_option('Droid','yampl_socket_name'):
         self.yampl_socket_name = self.config.get('Droid','yampl_socket_name').format(rank_num='%03d' % self.rank)
      else:
         logger.error('must specify "yampl_socket_name" in "Droid" section of config file')
         return
      if self.rank == 0:
         logger.info('Droid yampl_socket_name: %s',self.yampl_socket_name)

      # get get_more_events_threshold
      if self.config.has_option(config_section,'get_more_events_threshold'):
         self.get_more_events_threshold = self.config.getint(config_section,'get_more_events_threshold')
      else:
         logger.error('must specify "get_more_events_threshold" in "%s" section of config file',config_section)
         return
      if self.rank == 0:
         logger.info('JobComm get_more_events_threshold: %d',self.get_more_events_threshold)




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
      athpayloadcomm = athena_payloadcommunicator(self.yampl_socket_name,prelog=self.prelog)
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

   def __init__(self,socketname='EventService_EventRanges',context='local',prelog=''):

      # get current rank
      self.rank                        = MPI.COMM_WORLD.Get_rank()

      # the prelog is just a string to attach before each log message
      self.prelog                      = '%s| Rank %03i:' % (self.__class__.__name__,self.rank)
      
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
         logger.exception("%s Failed to send yampl message: %s",message)
         raise

   def recv(self):
      # receive yampl message
      size, buf = self.socket.try_recv_raw()
      if size == -1:
         return ''
      return str(buf)



# testing this thread
if __name__ == '__main__':
   logging.basicConfig(level=logging.DEBUG,
         format='%(asctime)s|%(process)s|%(thread)s|%(levelname)s|%(name)s|%(funcName)s|%(message)s',
         datefmt='%Y-%m-%d %H:%M:%S')
   logging.info('Start test of JobComm')
   import time
   #import argparse
   #oparser = argparse.ArgumentParser()
   #oparser.add_argument('-l','--jobWorkingDir', dest="jobWorkingDir", default=None, help="Job's working directory.",required=True)
   #args = oparser.parse_args()

   queues = {'FileManager':SerialQueue.SerialQueue(),
             'JobComm':SerialQueue.SerialQueue()
            }

   job_def = {
      "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
      "PandaID": 3298217817,
      "StatusCode": 0,
      "attemptNr": 3,
      "checksum": "ad:363a57ab",
      "cloud": "WORLD",
      "cmtConfig": "x86_64-slc6-gcc47-opt",
      "coreCount": 8,
      "currentPriority": 851,
      "ddmEndPointIn": "NERSC_DATADISK",
      "ddmEndPointOut": "LRZ-LMU_DATADISK,NERSC_DATADISK",
      "destinationDBlockToken": "dst:LRZ-LMU_DATADISK,dst:NERSC_DATADISK",
      "destinationDblock": "mc15_13TeV.362002.Sherpa_CT10_Znunu_Pt0_70_CVetoBVeto_fac025.simul.HITS.e4376_s3022_tid10919503_00_sub0384058277,mc15_13TeV.362002.Sherpa_CT10_Znunu_Pt0_70_CVetoBVeto_fac025.simul.log.e4376_s3022_tid10919503_00_sub0384058278",
      "destinationSE": "LRZ-LMU_C2PAP_MCORE",
      "dispatchDBlockToken": "NULL",
      "dispatchDBlockTokenForOut": "NULL,NULL",
      "dispatchDblock": "panda.10919503.03.15.GEN.c2a897d6-ea51-4054-83a5-ce0df170c6e1_dis003287071386",
      "eventService": "True",
      "fileDestinationSE": "LRZ-LMU_C2PAP_MCORE,NERSC_Edison",
      "fsize": "24805997",
      "homepackage": "AtlasProduction/19.2.5.3",
      "inFilePaths": "/scratch2/scratchdirs/dbenjami/harvester_edison/test-area/test-18/EVNT.06402143._000615.pool.root.1",
      "inFiles": "EVNT.06402143._000615.pool.root.1",
      "jobDefinitionID": 0,
      "jobName": "mc15_13TeV.362002.Sherpa_CT10_Znunu_Pt0_70_CVetoBVeto_fac025.simul.e4376_s3022.3268661856",
      "jobPars": "--inputEVNTFile=EVNT.06402143._000615.pool.root.1 --AMITag=s3022 --DBRelease=\"default:current\" --DataRunNumber=222525 --conditionsTag \"default:OFLCOND-RUN12-SDR-19\" --firstEvent=1 --geometryVersion=\"default:ATLAS-R2-2015-03-01-00_VALIDATION\" --maxEvents=1000 --outputHITSFile=HITS.10919503._000051.pool.root.1 --physicsList=FTFP_BERT --postInclude \"default:PyJobTransforms/UseFrontier.py\" --preInclude \"EVNTtoHITS:SimulationJobOptions/preInclude.BeamPipeKill.py,SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py,AthenaMP/AthenaMP_EventService.py\" --randomSeed=611 --runNumber=362002 --simulator=MC12G4 --skipEvents=0 --truthStrategy=MC15aPlus",
      "jobsetID": 3287071385,
      "logFile": "log.10919503._000051.job.log.tgz.1.3298217817",
      "logGUID": "6872598f-658b-4ecb-9a61-0e1945e44dac",
      "maxCpuCount": 46981,
      "maxDiskCount": 323,
      "maxWalltime": 46981,
      "minRamCount": 23750,
      "nSent": 1,
      "nucleus": "LRZ-LMU",
      "outFiles": "HITS.10919503._000051.pool.root.1,log.10919503._000051.job.log.tgz.1.3298217817",
      "processingType": "validation",
      "prodDBlockToken": "NULL",
      "prodDBlockTokenForOutput": "NULL,NULL",
      "prodDBlocks": "mc15_13TeV:mc15_13TeV.362002.Sherpa_CT10_Znunu_Pt0_70_CVetoBVeto_fac025.evgen.EVNT.e4376/",
      "prodSourceLabel": "managed",
      "prodUserID": "glushkov",
      "realDatasets": "mc15_13TeV.362002.Sherpa_CT10_Znunu_Pt0_70_CVetoBVeto_fac025.simul.HITS.e4376_s3022_tid10919503_00,mc15_13TeV.362002.Sherpa_CT10_Znunu_Pt0_70_CVetoBVeto_fac025.simul.log.e4376_s3022_tid10919503_00",
      "realDatasetsIn": "mc15_13TeV:mc15_13TeV.362002.Sherpa_CT10_Znunu_Pt0_70_CVetoBVeto_fac025.evgen.EVNT.e4376/",
      "scopeIn": "mc15_13TeV",
      "scopeLog": "mc15_13TeV",
      "scopeOut": "mc15_13TeV",
      "sourceSite": "NULL",
      "swRelease": "Atlas-19.2.5",
      "taskID": 10919503,
      "transferType": "NULL",
      "transformation": "Sim_tf.py"
   }

   eventranges = [
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-1-49",
         "lastEvent": 1,
         "scope": "mc15_13TeV",
         "startEvent": 1
      },
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-2-49",
         "lastEvent": 2,
         "scope": "mc15_13TeV",
         "startEvent": 2
      },
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-3-49",
         "lastEvent": 3,
         "scope": "mc15_13TeV",
         "startEvent": 3
      },
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-4-49",
         "lastEvent": 4,
         "scope": "mc15_13TeV",
         "startEvent": 4
      },
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-5-49",
         "lastEvent": 5,
         "scope": "mc15_13TeV",
         "startEvent": 5
      },
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-6-49",
         "lastEvent": 6,
         "scope": "mc15_13TeV",
         "startEvent": 6
      },
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-7-49",
         "lastEvent": 7,
         "scope": "mc15_13TeV",
         "startEvent": 7
      },
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-8-49",
         "lastEvent": 8,
         "scope": "mc15_13TeV",
         "startEvent": 8
      }
   ]
   
   jc = JobComm(queues,5)

   # setup payloadcommunicator
   socket = yampl.ClientSocket('EventService_EventRanges', 'local')

   jc.start()


   ######
   ## Test event range request and receive
   ############

   # acting like JobManager   
   # put job message on queue
   msg = {'type':MessageTypes.NEW_JOB,'job':job_def}
   queues['JobComm'].put(msg)


   # acting like AthenaMP
   # wait for JobComm to start yampl server or else messages will be missed.
   time.sleep(2)
   # send the Ready for Events as many times as we have event ranges
   logger.info('AthenaMP workers sending %d messages "Ready for Events"',len(eventranges))
   for i in range(len(eventranges)):
      socket.send_raw(JobComm.ATHENA_READY_FOR_EVENTS)

   
   # acting like Yoda/WorkManager
   
   # use mpi to wait for event range request
   logger.info('wait for MPI mesage from JobComm sent to Yoda/WorkManager')
   req = ydm.recv_eventranges_request()
   status = MPI.Status()
   msg = req.wait(status=status)
   logger.info('received MPI message from JobComm: %s',msg)

   if msg['type'] != MessageTypes.REQUEST_EVENT_RANGES:
      raise Exception
   # send event ranges in response and wait for send to complete
   logger.info('Yoda/WorkManager is sending event ranges to JobComm')
   ydm.send_droid_new_eventranges(eventranges,status.Get_source()).wait()

   # acting like AthenaMP
   
   # receive event range on yampl
   size,msg = socket.try_recv_raw()
   while size == -1:
      size,msg = socket.try_recv_raw()
      time.sleep(1)
   msg = serializer.deserialize(msg)
   logger.info('AthenaMP worker received event range: %s',msg)



   ######
   ## Test output file messages
   ############
   for eventrange_index in range(len(eventranges)):

      # acting like AthenaMP
      logger.info('sending AthenaMP worker message for output file')
      msg = "%s,%s,CPU:1,WALL:1" % (eventranges[eventrange_index]['LFN'].replace('EVNT','HITS'),eventranges[eventrange_index]['eventRangeID'])
      socket.send_raw(msg)
      eventrange_index += 1

      # acting like FileManager
      logger.info('FileManager is waiting for file output message from JobComm')
      msg = queues['FileManager'].get()
      logger.info('FileManager received message from JobComm: %s',msg)

   
   ######
   ## Test sending JobComm NO_MORE_EVENTRANGES Message
   ############

   # acting like yoda

   # use mpi to wait for event range request
   logger.info('wait for MPI mesage from JobComm sent to Yoda/WorkManager')
   req = ydm.recv_eventranges_request()
   status = MPI.Status()
   msg = req.wait(status=status)
   logger.info('received MPI message from JobComm: %s',msg)

   if msg['type'] != MessageTypes.REQUEST_EVENT_RANGES:
      raise Exception
   # send event ranges in response and wait for send to complete
   logger.info('Yoda/WorkManager is sending event ranges to JobComm')
   ydm.send_droid_no_eventranges_left(status.Get_source()).wait()


   #jc.stop()

   jc.join()

   logger.info('exiting test')









