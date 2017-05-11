import os,sys,threading,logging,shutil
from pandayoda.common import MessageTypes,serializer,SerialQueue,yoda_droid_messenger as ydm
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
            if request_from_yoda is None:
               logger.debug('%s receive message from yoda',self.prelog)
               request_from_yoda = ydm.receive_message(source=ydm.YODA_RANK,tag=ydm.FROM_YODA)
            
            # check if requested message has arrived
            if request_from_yoda is not None:
               flag,data = request_from_yoda.test()
               if flag:
                  # received data
                  logger.debug('%s received data from yoda: type=%s',self.prelog,data['type'])

                  # route the message to where it needs to go
                  # if the routing fails, the request stays unchanged so it can be tried up again
                  request_from_yoda = self.route_message(data,request_from_yoda)

               else:
                  # no data received, move on
                  logger.debug('%s waiting for response from yoda',self.prelog)
                  continue


            logger.debug('%s received message from yoda: %s ',self.prelog,data)

         else:
            logger.debug('%s received message from YodaComm: %s',self.prelog,msg)

            if msg['type'] == MessageTypes.REQUEST_JOB:
               # recieved a request for a new Panda job
               # YodaComm will send a message to Yoda for a new panda job
               logger.debug('%s sending yoda request for new job, blocking on send',self.prelog)
               request_to_yoda = ydm.send_job_request()
               request_to_yoda.wait()
               request_to_yoda = None
            elif msg['type'] == MessageTypes.REQUEST_EVENT_RANGES:
               # received a request for a new Event Range
               logger.debug('%s sending request for new event range',self.prelog)
               request_to_yoda = ydm.send_eventrange_request()
               request_to_yoda.wait()
               request_to_yoda = None

      logger.info('%s YodaComm exiting',self.prelog)

   
   def route_message(self,msg,request_from_yoda):           
         
      # received new job
      if msg['type'] == MessageTypes.NEW_JOB:
         logger.debug('%s sending message to JobManager',self.prelog)
         # send on to JobManager
         try:
            self.queues['JobManager'].put(msg)
         except SerialQueue.Full:
            logger.warning('%s JobManager queue full, will wait to send message')
         else:
            # this request has been fulfilled so reset it to None
            request_from_yoda = None

      # received new event range
      elif (msg['type'] == MessageTypes.NEW_EVENT_RANGES 
         or msg['type'] == MessageTypes.NO_MORE_EVENT_RANGES):
         logger.debug('%s sending message to JobComm',self.prelog)
         # send on to JobComm
         try:
            self.queues['JobComm'].put(msg)
         except SerialQueue.Full:
            logger.warning('%s JobComm queue full, will wait to send message')
         else:
            # this request has been fulfilled so reset it to None
            request_from_yoda = None
            
      return request_from_yoda







