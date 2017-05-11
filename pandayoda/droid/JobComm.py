import os,sys,logging,threading,subprocess,time
from pandayoda.common import MessageTypes,serializer,SerialQueue
try:
   import yampl
except:
   logger.exception("Failed to import yampl")
   raise

from mpi4py import MPI
logger = logging.getLogger(__name__)

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
                loopTimeout         = 30,
                yampl_socket_name   = 'EventService_EventRanges'):
      ''' 
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send 
                     messages to other Droid components about errors, etc.
        loopTimeout: A positive number of seconds to block between loop executions.'''
      # call base class init function
      super(JobComm,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # the timeout duration when no message was received from AthenaMP
      self.loopTimeout           = loopTimeout

      # socket name used by AthenaMP to connect, needs to be the same between here and AthenaMP
      self.yampl_socket_name     = yampl_socket_name

      # get current rank
      self.rank                  = MPI.COMM_WORLD.Get_rank()

      # the prelog is just a string to attach before each log message
      self.prelog                = 'Rank %03i:' % self.rank

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

      while not self.exit.isSet():
         logger.debug('%s start loop',self.prelog)
         
         # try to receive a message from AthenaMP
         msg = comm.recv()
         if len(msg) > 0:
            logger.debug('%s received message: %s',self.prelog,msg)
            # received a message, parse it
            if JobComm.ATHENA_READY_FOR_EVENTS in msg:
               # Athena is ready for events so ask Droid for an event range
               msg = {'type':MessageTypes.REQUEST_EVENT_RANGES}
               try:
                  self.queues['YodaComm'].put(msg)
               except SerialQueue.Full:
                  logger.warning('%s YodaComm queue is full',self.prelog)
                  continue

               # get response from YodaComm
               inputmsg = self.queues['JobComm'].get()
               if   inputmsg['type'] == MessageTypes.NEW_EVENT_RANGES:
                  # event range is this format:
                  # [{"eventRangeID": "8848710-3005316503-6391858827-3-10", "LFN":"EVNT.06402143._012906.pool.root.1", "lastEvent": 3, "startEvent": 3, "scope": "mc15_13TeV", "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"}]
                  # in json format
                  eventranges = inputmsg['eventranges']
                  
                  # send event ranges to AthenaMP
                  comm.send(serializer.serialize(eventranges))
               elif inputmsg['type'] == MessageTypes.NO_MORE_EVENT_RANGES:
                  # no more event ranges left
                  comm.send(JobComm.NO_MORE_EVENTS)



            elif msg.startswith('/'):
               logger.debug('%s received output file',self.prelog)
               # Athena sent details of an output file
               # send the details to Droid
               parts = msg.split(',')
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
               else:
                  logger.error('%s received message from Athena that appears to be output file because it starts with "/" however when split by commas, it only has %i pieces when 4 are expected',self.prelog,len(parts))
            elif msg.startswith(JobComm.ATHENAMP_FAILED_PARSE):
               # Athena passed an error
               logger.error('%s AthenaMP failed to parse the message: %s',self.prelog,msg)
               pass
            elif msg.startswith('ERR'):
               # Athena passed an error
               logger.error('%s received error from AthenaMP: %s',self.prelog,msg)
               pass
            else:
               raise Exception('%s received mangled message from Athena: %s',self.prelog,msg)


         else:
            # no message received so sleep
            logger.debug('%s no yampl message from AthenaMP, sleeping.',self.prelog)
            time.sleep(self.loopTimeout)

      logger.info('%s JobComm exiting',self.prelog)


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

   queues = {'YodaComm':SerialQueue.SerialQueue(),'JobComm':SerialQueue.SerialQueue()}

   jc = JobComm(queues,5)

   # setup communicator
   socket = yampl.ClientSocket('EventService_EventRanges', 'local')

   jc.start()

   while True:
      logger.info('start loop')
      if not jc.isAlive(): break

      logger.info('send message')
      socket.send_raw(JobComm.ATHENA_READY_FOR_EVENTS)

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









