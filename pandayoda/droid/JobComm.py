import os,sys,logging,threading,subprocess,time
logger = logging.getLogger(__name__)

from pandayoda.common import MessageTypes,serializer,SerialQueue,EventRangeList
from pandayoda.common import yoda_droid_messenger as ydm

try:
   import yampl
except:
   logger.exception("Failed to import yampl")
   raise

from mpi4py import MPI

class NoEventRangeDefined(Exception): pass
class NoMoreEventsToProcess(Exception): pass
class FailedToParseYodaMessage(Exception): pass

class JobComm(threading.Thread):
   '''  JobComm: This thread handles all the AthenaMP related communication

Pilot/Droid launches AthenaMP and starts listening to its messages. AthenaMP finishes initialization and sends "Ready for events" to Pilot/Droid. Pilot/Droid sends back an event range. AthenaMP sends NWorkers-1 more times "Ready for events" to Pilot/Droid. On each of these messages Pilot/Droid replies with a new event range. After that all AthenaMP workers are busy processing events. Once some AthenaMP worker is available to take the next event range, AthenaMP sends "Ready for events" to Pilot/Droid. Pilot/Droid sends back an event range. Once some output file becomes available, AthenaMP sends full path, RangeID, CPU time and Wall time to Pilot/Droid and does not expect any answer on this message. Here is an example of such message:

"/build1/tsulaia/20.3.7.5/run-es/athenaMP-workers-AtlasG4Tf-sim/worker_1/myHITS.pool.root_000.Range-6,ID:Range-6,CPU:1,WALL:1"

If Pilot/Droid receives "Ready for events"and there are no more ranges to process, it answers with "No more events". This is a signal for AthenaMP that no more events are expected. In this case AthenaMP waits for all its workers to finish processing current event ranges, reports all outputs back to Pilot/Droid and exits.

The event range format is json and is this: [{"eventRangeID": "8848710-3005316503-6391858827-3-10", "LFN":"EVNT.06402143._012906.pool.root.1", "lastEvent": 3, "startEvent": 3, "scope": "mc15_13TeV", "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"}]

   '''

   ATHENA_READY_FOR_EVENTS = 'Ready for events'
   NO_MORE_EVENTS          = 'No more events'
   ATHENAMP_FAILED_PARSE   = 'ERR_ATHENAMP_PARSE'

   def __init__(self,queues,
                loopTimeout               = 30,
                yampl_socket_name         = 'EventService_EventRanges',
                get_more_events_threshold = 3):
      ''' 
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send 
                     messages to other Droid components about errors, etc.
        loopTimeout: A positive number of seconds to block between loop executions.'''
      # call base class init function
      super(JobComm,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                      = queues

      # the timeout duration when no message was received from AthenaMP
      self.loopTimeout                 = loopTimeout

      # socket name used by AthenaMP to connect, needs to be the same between here and AthenaMP
      self.yampl_socket_name           = yampl_socket_name

      # threshold at which to request more events
      self.get_more_events_threshold   = get_more_events_threshold

      # get current rank
      self.rank                        = MPI.COMM_WORLD.Get_rank()

      # the prelog is just a string to attach before each log message
      self.prelog                      = 'Rank %03i:' % self.rank

      # this is used to trigger the thread exit
      self.exit = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the JobManager thread to exit'''
      self.exit.set()

   def run(self):
      ''' this is the function run as a subthread when the user runs jobComm_instance.start() '''

      # setup communicator
      logger.debug('%s start communicator',self.prelog)
      comm = athena_communicator(self.yampl_socket_name,prelog=self.prelog)

      # dictionary of job definitions sent by the JobManager key-ed by PandaID
      job_defs = {}

      # EventRangeList obj
      eventranges = EventRangeList.EventRangeList()

      # this flag is set when it has been determined that the thread can exit
      # after all event ranges have been processed
      waiting_to_exit = False

      # this is the current yampl message from AthenaMP which requires a response
      # if it is set to None, a new message will be requested
      athenamp_msg = None



      while not self.exit.isSet():
         logger.debug('%s start loop',self.prelog)
         
         # check to see there is a job definition
         try:
            qmsg = self.queues['JobComm'].get(block=False)
            if qmsg['type'] == MessageTypes.NEW_JOB:
               if 'job' in qmsg:
                  job_defs[qmsg['job']['PandaID']] = qmsg['job']
               else:
                  logger.error('%s Received message of NEW_JOB tyep but no job in message: %s',self.prelog,qmsg)
            else:
               logger.error('%s Received message of unknown type: %s',self.prelog,qmsg)
         except SerialQueue.Empty:
            logger.debug('%s no jobs on JobComm queue',self.prelog)

         # if there has been no job definition sent yet, then sleep
         if len(job_defs) == 0:
            logger.debug('no job definition, sleeping %d',self.loopTimeout)
            time.sleep(self.loopTimeout)
            continue

         # check to get event ranges
         if eventranges.number_ready() < self.get_more_events_threshold:
            try:
               eventranges += self.get_eventranges()
            except FailedToParseYodaMessage:
               raise
            except NoEventRangeDefined:
               raise
            except NoMoreEventsToProcess:
               logger.info('%s Yoda says no more events to process.',self.prelog)
               waiting_to_exit = True
         
         # check if we should exit
         if waiting_to_exit and eventranges.number_processing() <= 0:
            self.stop()
            continue

         # receive a message from AthenaMP if we are not already processing one
         if athenamp_msg is None:
            athenamp_msg = comm.recv()
         
         # received a message, parse it
         if len(athenamp_msg) > 0:
            logger.debug('%s received message: %s',self.prelog,athenamp_msg)
            if JobComm.ATHENA_READY_FOR_EVENTS in athenamp_msg:
               # Athena is ready for events so ask Yoda for an event range
               logger.debug('%s received ready for events',self.prelog)
               
               # get the next event ranges to be processed
               try:
                  local_eventranges = eventranges.get_next()
               # no more event ranges available
               except EventRangeList.NoMoreEventRanges:
                  # if the waiting_to_exit flag is set, then tell the AthenaMP worker there are no more events
                  if waiting_to_exit:
                     comm.send(JobComm.NO_MORE_EVENTS)
                  # otherwise continue the loop so more events will be requested
                  else:
                     continue
               # something wrong with the index in the EventRangeList index
               except EventRangeList.RequestedMoreRangesThanAvailable:
                  continue

               # send AthenaMP the new event ranges
               comm.send(serializer.serialize(local_eventranges))
               # reset the message so a new one will be recieved
               athenamp_msg = None

            elif athenamp_msg.startswith('/'):
               # Athena sent details of an output file
               logger.debug('%s received output file',self.prelog)
               
               # parse message
               parts = athenamp_msg.split(',')
               # there should be four parts:
               # "/build1/tsulaia/20.3.7.5/run-es/athenaMP-workers-AtlasG4Tf-sim/worker_1/myHITS.pool.root_000.Range-6,ID:Range-6,CPU:1,WALL:1"
               if len(parts) == 4:
                  # parse the parts
                  outputfilename = parts[0]
                  eventrangeid = parts[1]
                  cpu = parts[2]
                  wallclock = parts[3]
                  msg = {'type':MessageTypes.OUTPUT_FILE,
                         'filename':outputfilename,
                         'eventrangeid':eventrangeid,
                         'cpu':cpu,
                         'wallclock':wallclock
                         }
                  # send them to droid
                  try:
                     self.queues['FileManager'].put(msg)
                  except SerialQueue.Full:
                     logger.warning('%s FileManager is full, could not send output info.',self.prelog)
                     continue # this will try again since we didn't reset the message to None
               else:
                  logger.error('%s received message from Athena that appears to be output file because it starts with "/" however when split by commas, it only has %i pieces when 4 are expected',self.prelog,len(parts))
               
               # reset message so another will be received
               athenamp_msg = None

            elif athenamp_msg.startswith(JobComm.ATHENAMP_FAILED_PARSE):
               # Athena passed an error
               logger.error('%s AthenaMP failed to parse the message: %s',self.prelog,athenamp_msg)
               athenamp_msg = None
               continue
            elif athenamp_msg.startswith('ERR'):
               # Athena passed an error
               logger.error('%s received error from AthenaMP: %s',self.prelog,athenamp_msg)
               athenamp_msg = None
               continue
            else:
               raise Exception('%s received mangled message from Athena: %s',self.prelog,athenamp_msg)


         else:
            # no message received so sleep
            logger.debug('%s no yampl message from AthenaMP, sleeping %d',self.prelog,self.loopTimeout)
            # reset message
            athenamp_msg = None

            time.sleep(self.loopTimeout)
            continue

      logger.info('%s JobComm exiting',self.prelog)

   def get_eventranges(self):
      # ask Yoda to send event ranges
      request = ydm.send_eventranges_request()
      # wait for yoda to receive message
      request.wait()

      # get response from Yoda
      request = ydm.recv_eventranges()
      # wait for yoda to respond
      flag,yoda_msg = request.wait()

      logger.debug('%s received message from yoda %s',self.prelog,yoda_msg)

      if yoda_msg['type'] == MessageTypes.NEW_EVENT_RANGES:
         
         if 'eventranges' in yoda_msg:
            return EventRangeList.EventRangeList(yoda_msg['eventranges'])
            #comm.send(serializer.serialize(yoda_msg['eventranges']))
         else:
            raise NoEventRangeDefined('%s no event ranges in message %s' %(self.prelog,yoda_msg))
      elif yoda_msg['type'] == MessageTypes.NO_MORE_EVENT_RANGES:
         # no more event ranges left
         logger.debug('%s no more events from yoda %s',self.prelog,yoda_msg)
         raise NoMoreEventsToProcess
      else:
         raise FailedToParseYodaMessage('%s incorrect message type in yoda message %s' % (self.prelog,yoda_msg))
      

class athena_communicator:
   ''' small class to handle yampl communication exception handling '''
   def __init__(self,socketname='EventService_EventRanges',context='local',prelog=''):

      self.prelog = prelog
      
      # create server socket for yampl
      try:
         self.socket = yampl.ServerSocket(socketname, context)
      except:
         logger.exception('%s failed to create yampl server socket',self.prelog)
         raise

   def send(self,message):
      # send message using yampl
      try:
         self.socket.send_raw(message)
      except:
         logger.exception("%s Failed to send yampl message: %s",self.prelog,message)
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
         format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
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

   # put job message on queue
   msg = {'type':MessageTypes.NEW_JOB,'job':job_def}
   queues['JobComm'].put(msg)

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

   # setup communicator
   socket = yampl.ClientSocket('EventService_EventRanges', 'local')

   jc.start()

   i = 0

   while True:
      logger.info('start loop')
      if not jc.isAlive(): break


      if i == 0:
         logger.info('send message "Ready for Events"')
         socket.send_raw(JobComm.ATHENA_READY_FOR_EVENTS)

         # use mpi to wait for event range request
         req = ydm.receive_message()
         status = MPI.Status()
         flag,msg = req.wait(status=status)

         if msg['type'] != MessageTypes.REQUEST_EVENT_RANGES:
            raise Exception
         # send event ranges in response and wait for send to complete
         ydm.send_droid_new_eventranges(eventranges,status.Get_source()).wait()


      elif i == 1:
         logger.info('send message for output file')
         msg = "/build1/tsulaia/20.3.7.5/run-es/athenaMP-workers-AtlasG4Tf-sim/worker_1/myHITS.pool.root_000.Range-6,ID:Range-6,CPU:1,WALL:1"
         socket.send_raw(msg)


      logger.info('get message')
      size,msg = socket.try_recv_raw()
      if size != -1:
         logger.info('received message: %s',msg)
         msg = serializer.deserialize(msg)
      else:
         time.sleep(5)





   jc.stop()

   jc.join()

   logger.info('exiting test')









