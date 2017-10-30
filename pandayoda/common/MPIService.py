import threading,logging,time
from pandayoda.common import MessageTypes
logger = logging.getLogger('MPIService')

from mpi4py import MPI

class MPIService(threading.Thread):
   ''' this thread class should be used for running MPI operations
       within a single MPI rank '''

   
   def __init__(self):
      # call init of Thread class
      super(MPIService,self).__init__()

      # dictionary of the queues for all running threads
      # inputs come via "self.queues['MPIService']" 
      self.queues                      = None

      # this map should have keys from the MessageTypes, and the value should be the thread queue name
      # to which to forward the message of that type. This must be overridden by the parent thread
      self.forwarding_map              = None

      # this is used to trigger the thread exit
      self.exit                        = threading.Event()

      # this is set when the thread should continue on to the full loop
      self.init_done                   = threading.Event()

      # loop_timeout decided loop sleep times
      self.loop_timeout                = 30


   def stop(self):
      ''' this function can be called by outside threads to cause the service thread to exit'''
      self.exit.set()

   def initialize(self,queues,forwarding_map,loop_timeout=30):
      # dictionary of the queues for all running threads
      # inputs come via "self.queues['MPIService']" 
      self.queues                      = queues

      # this map should have keys from the MessageTypes, and the value should be the thread queue name
      # to which to forward the message of that type. This must be overridden by the parent thread
      self.forwarding_map              = forwarding_map

      # loop_timeout decided loop sleep times
      self.loop_timeout                = loop_timeout

      self.init_done.set()

   def run(self):
      ''' run when obj.start() is called '''

     
      # wait for the init information to be set
      self.init_done.wait()

      self.receiveRequest = None
      
      while not self.exit.isSet():
         logger.debug('starting loop, queue size = %s',self.queues['MPIService'].qsize())

         # check for incoming message
         message = self.receive_message()
         # if message received forward it on
         if message: 
            logger.debug('received MPI message: %s',message)
            self.forward_message(message)


         
         # check for messages on the queue that need to be sent
         while not self.queues['MPIService'].empty():
            qmsg = self.queues['MPIService'].get()
            
            logger.debug('received message: %s',qmsg)

            # determine if destination rank or tag was set
            if 'destination_rank' in qmsg:
               destination_rank = qmsg['destination_rank']
            else:
               logger.error('received message to send, but there is no destination_rank specified')
               continue
            tag = None
            if 'tag' in qmsg:
               tag = qmsg['tag']

            # send message
            send_request = None
            if destination_rank is not None and tag is not None:
               logger.debug('sending msg with destination and tag')
               send_request = MPI.COMM_WORLD.isend(qmsg,dest=destination_rank,tag=tag)
            elif destination_rank is not None:
               logger.debug('sending msg with destination')
               send_request = MPI.COMM_WORLD.isend(qmsg,dest=destination_rank)

            # wait for send to complete
            send_request.wait()

         if not message and self.queues['MPIService'].empty():
            time.sleep(2)




   def receive_message(self):
      # there should always be a request waiting for this rank to receive data
      if self.receiveRequest is None:
         self.receiveRequest = MPI.COMM_WORLD.irecv(source=MPI.ANY_SOURCE)
      # check status of current request
      if self.receiveRequest:
         logger.debug('check for MPI message')
         status = MPI.Status()
         # test to see if message was received
         message_received,message = self.receiveRequest.test(status=status)
         # if received reset and return source rank and message content
         if message_received:
            logger.debug('MPI message received: %s',message)
            self.receiveRequest = None
            # add source rank to the message
            message['source_rank'] = status.Get_source()
            return message
      # no message received return nothing
      return None

         
   def forward_message(self,message):
      thread_name = self.forwarding_map[message['type']]
      logger.debug('forwarding recieve MPI message to %s',thread_name)
      self.queues[thread_name].put(message)

# initialize the rank variables for other threads
rank = MPI.COMM_WORLD.Get_rank()
nranks = MPI.COMM_WORLD.Get_size()

mpiService = MPIService()
mpiService.start()


