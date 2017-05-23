import os,sys,threading,logging
from pandayoda.common import MessageTypes,SerialQueue,yoda_droid_messenger as ydm
logger = logging.getLogger(__name__)

class DroidComm(threading.Thread):
   ''' Droid Comm: this thread manages the communication with Droid '''
   def __init__(self,queues,
                config,
                loopTimeout      = 1):
      ''' 
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send 
                     messages to other Droid components about errors, etc.
        config: the ConfigParser handle for yoda
        loopTimeout: A positive number of seconds to block when retrieving 
                     a message from the inputQueue.'''
      # call base class init function
      super(DroidComm,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # configuration of Yoda
      self.config          = config
      
      # the timeout duration 
      self.loopTimeout           = loopTimeout

      # this is used to trigger the thread exit
      self.exit = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the DroidComm thread to exit'''
      self.exit.set()

   def run(self):
      ''' this function is executed as the subthread. '''

      request_from_droid = None

      while not self.exit.isSet():
         logger.debug('start loop')

         # check for message from Droids
         request_from_droid = ydm.get_droid_message()


         # check to see if message was received
         if request_from_droid is not None:
            mpi_status = MPI.Status()
            flag,data = request_from_droid.test(status=mpi_status)
            if flag:
               # received data

               # get source rank
               source_rank = status.Get_source()
               # add source_rank to the message
               data['source_rank'] = source_rank

               # reset request from droid
               request_from_droid = None

               logger.debug('received data from droid: type=%s source_rank=%d',data['type'],source_rank)

               # act on request 
               if data['type'] == MessageTypes.REQUEST_JOB or data['type'] == MessageTypes.REQUEST_EVENTRANGES:
                  # Droid is requesting a job, so forward the request to the work manager
                  self.queues['WorkManager'].put(data)
                  # increment work manager response counter
                  response_counter['WorkManager'] += 1
               else:
                  logger.error('failed to parse message from droid: type=%s source_rank=%d',data['type'],source_rank)


            else:
               # no data received, move on
               logger.debug('waiting for response from droid')
         
         # check for message from other Yoda components
         try:
            msg = self.queues['DroidComm'].get(timeout=self.loopTimeout)
         except SerialQueue.Empty:
            logger.debug('no message on DroidComm queue')
         else:
            logger.debug('received message from DroidComm: %s',msg)

            # act on message
            if msg['type'] == MessageTypes.NEW_JOB:
               # send new job back to source droid rank
               
               sent_request = ydm.send_droid_new_job(msg['job'],msg['source_rank'])
               # wait for message to be received
               sent_request.wait()
            elif msg['type'] == MessageTypes.NEW_EVENTRANGES:
               # send new event ranges back to source droid rank

               sent_request = ydm.send_droid_new_eventranges(msg['eventranges'],msg['source_rank'])
               # wait for message to be received
               sent_request.wait()
            else:
               logger.error('failed to parse msg: %s',msg)





