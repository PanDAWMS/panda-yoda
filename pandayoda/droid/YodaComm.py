import os,sys,json,threading,logging,shutil
import MessageTypes,serializer,SerialQueue
import yoda_droid_messenger as ydm
from mpi4py import MPI
logger = logging.getLogger(__name__)

class YodaComm(threading.Thread):
   ''' Yoda Comm: this thread manages the communication with Yoda '''
   def __init__(self,queues,
                output_file_path,
                loopTimeout      = 30):
      ''' 
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send 
                     messages to other Droid components about errors, etc.
        outputFilePath: Where event files will be copied
        inputQueueTimeout: A positive number of seconds to block when retrieving 
                     a message from the inputQueue.'''
      # call base class init function
      super(YodaComm,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # the timeout duration 
      self.loopTimeout           = loopTimeout

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
      ''' this is the function run as a subthread when the user runs yodaComm_instance.start() '''

      # this flag is set after a signal is sent to yoda requesting a job
      waiting_for_job = False

      # dictionary of ongoing requests
      request_from_yoda = None
      request_to_yoda = None

      while not self.exit.isSet():
         logger.debug('%s start loop',self.prelog)
         # YodaComm should begin by blocking on its message queue.
         # The JobManager should start up, and ask for a panda job first.

         try:
            msg = self.queues['YodaComm'].get(timeout=self.loopTimeout)
         except SerialQueue.Empty:
            logger.debug('%s no message on YodaComm queue',self.prelog)

            # no message on the queue, check if yoda has sent a message
            
            # if we are not waiting for a message from yoda, request a message
            if request_from_yoda is not None:
               logger.debug('%s receive message from yoda',self.prelog)
               request_from_yoda = ydm.receive_message(source=ydm.YODA_RANK,tag=ydm.FROM_YODA)
            
            # check if requested message has arrived
            if request_from_yoda is None:
               flag,data = request.test()
               if flag:
                  # received data
                  logger.debug('%s received data from yoda: %s',self.prelog,data)
               else:
                  # no data received, move on
                  continue


            logger.debug('%s received message from yoda: %s ',self.prelog,data)

         else:
            logger.debug('%s received message from YodaComm: %s',self.prelog,msg)

            if msg['type'] == MessageTypes.SEND_NEW_PANDA_JOB:
               # recieved a request for a new Panda job
               # YodaComm will send a message to Yoda for a new panda job
               logger.debug('%s sending yoda request for new job, blocking on send',self.prelog)
               request_to_yoda = ydm.send_job_request()
               request_to_yoda.wait()
            elif msg['type'] == MessageTypes.SEND_NEW_EVENT_RANGE:
               # received a request for a new Event Range
               logger.debug('%s sending request for new event range',self.prelog)

               
         
            








