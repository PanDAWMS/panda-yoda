import threading,logging,time,os,sys,copy
from pandayoda.common import StatefulService,SerialQueue
from mpi4py import MPI
logger = logging.getLogger('MPIService')


config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


class MPIService(StatefulService.StatefulService):
   ''' this thread class should be used for running MPI operations
       within a single MPI rank '''

   CREATED                    = 'CREATED'
   INITILIZED                 = 'INITILIZED'
   
   # equal priority given to MPI and Message Queues
   BALANCED_MODE              = 'BALANCED_MODE'
   # block on receiving MPI messages
   MPI_BLOCKING_MODE          = 'MPI_BLOCKING_MODE'
   # block on receiving Queue messages
   QUEUE_BLOCKING_MODE        = 'QUEUE_BLOCKING_MODE'

   EXITED                     = 'EXITED'

   STATES = [CREATED,INITILIZED,BALANCED_MODE,MPI_BLOCKING_MODE,QUEUE_BLOCKING_MODE,EXITED]



   
   def __init__(self,loop_timeout=1):
      # call init of Thread class
      super(MPIService,self).__init__(loop_timeout)

      # dictionary of the queues for all running threads
      # inputs come via "self.queues['MPIService']"
      self.queues                      = None

      # this map should have keys from the MessageTypes, and the value should be a list of the thread queue names
      # to which to forward the message of that type. This must be overridden by the parent thread
      # example: {MessageType.NEW_JOB: ['JobComm','Droid']}
      self.forwarding_map              = None

      # this is used to trigger the thread exit
      self.exit                        = threading.Event()

      # this is set when the thread should continue on to the full loop
      self.init_done                   = threading.Event()

      self.set_state(self.CREATED)

   def set_balanced(self):
      self.set_state(self.BALANCED_MODE)

   def in_balanced(self):
      return self.in_state(self.BALANCED_MODE)

   def set_mpi_blocking(self):
      self.set_state(self.MPI_BLOCKING_MODE)

   def in_mpi_blocking(self):
      return self.in_state(self.MPI_BLOCKING_MODE)

   def set_queue_blocking(self):
      self.set_state(self.QUEUE_BLOCKING_MODE)

   def in_queue_blocking(self):
      return self.in_state(self.QUEUE_BLOCKING_MODE)


   def initialize(self,config,queues,forwarding_map,loop_timeout=30):
      # dictionary of the queues for all running threads
      # inputs come via "self.queues['MPIService']"
      self.queues                      = queues

      # configuration of Yoda
      self.config                      = config

      # this map should have keys from the MessageTypes, and the value should be the thread queue name
      # to which to forward the message of that type. This must be overridden by the parent thread
      self.forwarding_map              = forwarding_map

      # loop_timeout decided loop sleep times
      self.loop_timeout                = loop_timeout

      # read config file
      self.read_config()

      self.set_state(self.BALANCED_MODE)
      self.init_done.set()

   def message_queue_empty(self):
      return self.queues['MPIService'].empty()

   def run(self):
      ''' run when obj.start() is called '''

     
      # wait for the init information to be set
      self.init_done.wait()

      self.receiveRequest = None
      self.loop_timeout = 2

      logger.info('loop_timeout = %s',self.loop_timeout)
      logger.debug('file: %s',__file__)

      self.send_requests = []

      # keep track of when a message arrived previously
      # Only perform blocking receives when no message was
      # received during the previous loop
      no_message_on_last_loop = False
      
      while not self.exit.isSet():
         logger.debug('starting loop, queue empty = %s, state = %s',self.queues['MPIService'].empty(),self.get_state())

         # check for incoming message
         logger.debug('check for MPI message')
         if no_message_on_last_loop and self.in_mpi_blocking():
            logger.debug('block on mpi for %s',self.loop_timeout)
            message = self.receive_message(block=True,timeout=self.loop_timeout)
         elif no_message_on_last_loop and self.in_balanced() and self.queues['MPIService'].empty():
            logger.debug('block on mpi for %s',self.loop_timeout / 2)
            message = self.receive_message(block=True,timeout=self.loop_timeout / 2)
         else:
            message = self.receive_message()
         
         # if message received forward it on
         if message is not None:
            # record that we received a message this loop
            no_message_on_last_loop = False
            # shorten our message for printing
            if logger.getEffectiveLevel() == logging.DEBUG:
               tmpmsg = str(message)
               if len(tmpmsg) > self.debug_message_char_length:
                  tmpslice = slice(0,self.debug_message_char_length)
                  tmpmsg = tmpmsg[tmpslice] + '...'
               logger.debug('received mpi message: %s',tmpmsg)
            # forward message
            self.forward_message(message)
         else:
            logger.debug('no message from MPI')


         
         # check for messages on the queue that need to be sent
         try:
            if no_message_on_last_loop and self.in_queue_blocking():
               logger.debug('block on queue for %s',self.loop_timeout)
               qmsg = self.queues['MPIService'].get(block=True,timeout=self.loop_timeout)
            elif no_message_on_last_loop and self.in_balanced():
               logger.debug('block on queue for %s',self.loop_timeout / 2)
               qmsg = self.queues['MPIService'].get(block=True,timeout=self.loop_timeout / 2)
            else:
               qmsg = self.queues['MPIService'].get(block=False)

            # record that we received a message this loop
            no_message_on_last_loop = False
            
            # shorten our message for printing
            if logger.getEffectiveLevel() == logging.DEBUG:
               tmpmsg = str(qmsg)
               if len(tmpmsg) > self.debug_message_char_length:
                  tmpslice = slice(0,self.debug_message_char_length)
                  tmpmsg = tmpmsg[tmpslice] + '...'
               logger.debug('received queue message: %s',tmpmsg)

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
            msgbuff = copy.deepcopy(qmsg)
            logger.debug('sending msg (of size %s) with destination %s and tag %s',sys.getsizeof(msgbuff),destination_rank,tag)
            if tag is None:
               send_request = MPI.COMM_WORLD.isend(msgbuff,dest=destination_rank)
            else:
               send_request = MPI.COMM_WORLD.isend(msgbuff,dest=destination_rank,tag=tag)

            # On Theta I saw strange MPI behavior when waiting for the request
            # from a non-blocking send (isend) which caused upto 20minute waits.
            # This request should only be waiting for MPI to copy my data into
            # its own buffer, but takes too long. This is a stop gap, which just
            # appends the message to a dictionary (after a deepcopy of the original)
            # and then moves on. It doesn't come back to check if it completed.
            # I should eventually make this an 'optional' patch to enable and disable.
            self.send_requests.append({'msg':msgbuff,'dest':destination_rank,'tag':tag,'req':send_request})
            
            # This was the previous code, which properly checks the isend request
            # has completed.

            # wait for send to complete
            #logger.debug('wait for send to complete')
            #send_request.wait()
            #logger.debug('send complete')
            
         except SerialQueue.Empty:
            logger.debug('no message from message queue')

            # record no messages received
            if message is None:
               logger.debug('no messages received this loop')
               no_message_on_last_loop = True

   def receive_message(self,block=False,timeout=None):
      # there should always be a request waiting for this rank to receive data
      logger.debug('receive_message: entering function')
      if self.receiveRequest is None:
         logger.debug('receive_message: creating request')
         self.receiveRequest = MPI.COMM_WORLD.irecv(self.default_message_buffer_size,MPI.ANY_SOURCE)
      # check status of current request
      if self.receiveRequest is not None:
         logger.debug('receive_message: checking request state')
         starttime = time.time()
         logger.debug('receive_message: enter block loop, block = %s, timeout = %s',block,timeout)
         while True:
            # logger.debug('check for MPI message')
            status = MPI.Status()
            # test to see if message was received
            logger.debug('receive_message: test request')
            message_received,message = self.receiveRequest.test(status=status)
            logger.debug('receive_message: done testing')
            # if received reset and return source rank and message content
            if message_received:
               # logger.debug('MPI message received: %s',message)
               self.receiveRequest = None
               # add source rank to the message
               message['source_rank'] = status.Get_source()
               return message

            duration = time.time() - starttime
            if not block or duration > timeout:
               logger.debug('receive_message: receive timed out exiting blocking MPI receive loop')
               return None
            elif not self.queues['MPIService'].empty():
               logger.debug('receive_message: message queue has input exiting blocking MPI receive loop')
               return None
            else:
               # logger.debug('sleep 1 second: block=%s timeout=%s time=%s',block,timeout,duration)
               time.sleep(1)
      # no message received return nothing
      return None

   def forward_message(self,message):
      
      if message['type'] in self.forwarding_map:
         for thread_name in self.forwarding_map[message['type']]:
            logger.debug('forwarding recieve MPI message to %s',thread_name)
            self.queues[thread_name].put(message)
      else:
         logger.warning('received message type with no forwarding defined: %s',message['type'])


   def read_config(self):
      # get self.default_message_buffer_size
      if self.config.has_option(config_section,'default_message_buffer_size'):
         self.default_message_buffer_size = self.config.getint(config_section,'default_message_buffer_size')
      else:
         logger.error('must specify "default_message_buffer_size" in "%s" section of config file',config_section)
         return
      logger.info('default_message_buffer_size: %d',self.default_message_buffer_size)

      # get self.debug_message_char_length
      if self.config.has_option(config_section,'debug_message_char_length'):
         self.debug_message_char_length = self.config.getint(config_section,'debug_message_char_length')
      else:
         default = 100
         logger.warning('no "debug_message_char_length" in "%s" section of config file, using default %s',config_section,default)
         self.debug_message_char_length = default
      logger.info('debug_message_char_length: %d',self.debug_message_char_length)


      # read yoda log level:
      if self.config.has_option(config_section,'loglevel'):
         self.loglevel = self.config.get(config_section,'loglevel')
         logger.info('%s loglevel: %s',config_section,self.loglevel)
         logger.setLevel(logging.getLevelName(self.loglevel))
      else:
         logger.warning('no "loglevel" in "%s" section of config file, keeping default',config_section)


# initialize the rank variables for other threads
rank = MPI.COMM_WORLD.Get_rank()
nranks = MPI.COMM_WORLD.Get_size()

mpiService = MPIService()
mpiService.start()


