#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - ..

import logging
import time
import sys
import copy
from pandayoda.common import MessageTypes
from pandayoda.common import VariableWithLock
from Queue import Empty
from pandayoda.common import StatefulService


class MPIService(StatefulService.StatefulService):
    """ this thread class should be used for running MPI operations
        within a single MPI rank """

    CREATED = 'CREATED'
    INITILIZED = 'INITILIZED'

    # equal priority given to MPI and Message Queues
    BALANCED_MODE = 'BALANCED_MODE'
    # block on receiving MPI messages
    MPI_BLOCKING_MODE = 'MPI_BLOCKING_MODE'
    # block on receiving Queue messages
    QUEUE_BLOCKING_MODE = 'QUEUE_BLOCKING_MODE'

    EXITED = 'EXITED'

    STATES = [CREATED, INITILIZED, BALANCED_MODE, MPI_BLOCKING_MODE, QUEUE_BLOCKING_MODE, EXITED]

    def __init__(self, queue_list, queue_map,
                 loglevel = 'INFO',
                 debug_message_char_length = 200,
                 default_message_buffer_size = 10000000,
                 loop_timeout = 30,
                 ):
        # call init of Thread class
        super(MPIService,self).__init__(loop_timeout)

        # a list of queues to send out messages to other processes
        self.queue_list = queue_list
        self.queue_map  = queue_map
        self.queue_map_set = StatefulService.Event()

        self.rank = VariableWithLock.VariableWithLock(-1)
        self.worldsize = VariableWithLock.VariableWithLock(-1)

        self.loglevel = loglevel
        self.debug_message_char_length = debug_message_char_length
        self.default_message_buffer_size = default_message_buffer_size

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

    def message_queue_empty(self):
        return self.queues['MPIService'].empty()

    def queue_map_is_set(self):
        self.queue_map_set.set()

    # this runs when 'instance.start()' is called
    def run(self):
        """ this is the function called when the user runs instance.start() """

        try:
            self.subrun()
        except Exception as e:
            import traceback
            traceback.print_exc()
            from mpi4py import MPI
            print('Rank %05d: uncaught exception in MPIService. Aborting all ranks: %s' % (MPI.COMM_WORLD.Get_rank(), e))
            MPI.COMM_WORLD.Abort()
            import sys
            sys.exit(-1)

    def subrun(self):
        """ this function is the business logic, but wrapped in exception """

        # this would be the first import of MPI
        # resutling in MPI_INIT being called
        from mpi4py import MPI
        self.MPI = MPI

        string = 'from MPI: %s of %s\n' % (MPI.COMM_WORLD.Get_rank(), MPI.COMM_WORLD.Get_size())
        string += 'from mpirank: %s of %s\n' % (self.rank.get(), self.worldsize.get())

        self.rank.set(MPI.COMM_WORLD.Get_rank())
        self.worldsize.set(MPI.COMM_WORLD.Get_size())

        string += ('from mpirank: %s of %s\n' % (self.rank.get(), self.worldsize.get()))
        string += ('from MPI: %s of %s\n' % (MPI.COMM_WORLD.Get_rank(), MPI.COMM_WORLD.Get_size()))

        self.queue_map_set.wait()
        self.logger = logging.getLogger(__name__)
        logging_format = '%(asctime)s|%(process)s|%(thread)s|' + ('%05d' % self.rank.get()) + '|%(levelname)s|%(name)s|%(message)s'
        logging_datefmt = '%Y-%m-%d %H:%M:%S'
        logging_filename = 'yoda_droid_%05d.log' % self.rank.get()
        for h in logging.root.handlers:
            logging.root.removeHandler(h)
        logging.basicConfig(level=self.loglevel,
                            format=logging_format,
                            datefmt=logging_datefmt,
                            filename=logging_filename)

        self.logger.info('string = %s',string)


        self.logger.info('setup rank %s of %s',self.rank.get(), self.worldsize.get())
        self.logger.info('queue_map:                   %s', self.queue_map)

        # build queues object:
        self.queues = {}
        for key in self.queue_map.keys():
            self.queues[key] = self.queue_list[self.queue_map[key]]

        # set forwarding_map
        self.forwarding_map = {}
        if self.rank.get() == 0:
            self.forwarding_map = MessageTypes.forwarding_map[0]
        else:
            self.forwarding_map = MessageTypes.forwarding_map[1]

        # set logging level here
        self.logger.setLevel(self.loglevel)
        self.logger.debug('file:                       %s', __file__)
        self.logger.info('loglevel:                    %s', self.loglevel)
        self.logger.info('debug_message_char_length:   %s', self.debug_message_char_length)
        self.logger.info('default_message_buffer_size: %s', self.default_message_buffer_size)
        self.logger.info('loop_timeout:                %s', self.loop_timeout)

        self.receiveRequest = None

        self.send_requests = []

        # set initial state
        self.set_balanced()

        # keep track of when a message arrived previously
        # Only perform blocking receives when no message was
        # received during the previous loop
        no_message_on_last_loop = False

        self.logger.info('from mpirank: %s of %s' % (self.rank.get(),self.worldsize.get()))
        self.logger.info('from MPI: %s of %s' % (MPI.COMM_WORLD.Get_rank(),MPI.COMM_WORLD.Get_size()))

        while not self.exit.is_set():
            self.logger.debug('starting loop, queue empty = %s, state = %s',
                              self.queues['MPIService'].empty(),self.get_state())

            # check for incoming message
            if no_message_on_last_loop and self.in_mpi_blocking():
                self.logger.info('block on mpi for %s',self.loop_timeout)
                message = self.receive_message(block=True,timeout=self.loop_timeout)
            elif no_message_on_last_loop and self.in_balanced() and self.queues['MPIService'].empty():
                self.logger.info('block on mpi for %s',self.loop_timeout / 2)
                message = self.receive_message(block=True,timeout=self.loop_timeout / 2)
            else:
                self.logger.info('check for mpi message')
                message = self.receive_message()

            # if message received forward it on
            if message is not None:
                # record that we received a message this loop
                no_message_on_last_loop = False
                # shorten our message for printing
                if self.logger.getEffectiveLevel() == logging.DEBUG:
                    tmpmsg = str(message)
                    if len(tmpmsg) > self.debug_message_char_length:
                        tmpslice = slice(0,self.debug_message_char_length)
                        tmpmsg = tmpmsg[tmpslice] + '...'
                    self.logger.debug('received mpi message: %s',tmpmsg)
                # forward message
                self.forward_message(message)
            else:
                self.logger.info('no message from MPI')

            # check for messages on the queue that need to be sent
            try:
                if no_message_on_last_loop and self.in_queue_blocking():
                    self.logger.info('block on queue for %s',self.loop_timeout)
                    qmsg = self.queues['MPIService'].get(block=True,timeout=self.loop_timeout)
                elif no_message_on_last_loop and self.in_balanced():
                    self.logger.info('block on queue for %s',self.loop_timeout / 2)
                    qmsg = self.queues['MPIService'].get(block=True,timeout=self.loop_timeout / 2)
                else:
                    self.logger.info('check for queue message')
                    qmsg = self.queues['MPIService'].get(block=False)

                # record that we received a message this loop
                no_message_on_last_loop = False

                # shorten our message for printing
                if self.logger.getEffectiveLevel() == logging.DEBUG:
                    tmpmsg = str(qmsg)
                    if len(tmpmsg) > self.debug_message_char_length:
                        tmpslice = slice(0,self.debug_message_char_length)
                        tmpmsg = tmpmsg[tmpslice] + '...'
                    self.logger.debug('received queue message: %s',tmpmsg)

                # determine if destination rank or tag was set
                if 'destination_rank' in qmsg:
                    destination_rank = qmsg['destination_rank']
                else:
                    self.logger.error('received message to send, but there is no destination_rank specified')
                    continue
                tag = None
                if 'tag' in qmsg:
                    tag = qmsg['tag']

                # send message
                msgbuff = copy.deepcopy(qmsg)
                self.logger.info('sending msg of size %s bytes and type %s with destination %s',sys.getsizeof(msgbuff),msgbuff['type'],destination_rank)
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
                # self.logger.debug('wait for send to complete')
                # send_request.wait()
                # self.logger.debug('send complete')

            except Empty:
                self.logger.debug('no message from message queue')

                # record no messages received
                if message is None:
                    self.logger.debug('no messages received this loop')
                    no_message_on_last_loop = True

        self.logger.info('waiting for all ranks to reach this point before exiting')
        self.MPI.COMM_WORLD.Barrier()
        self.logger.info('exiting')

    def receive_message(self, block=False, timeout=None):
        # there should always be a request waiting for this rank to receive data
        self.logger.debug('receive_message: entering function')
        if self.receiveRequest is None:
            self.logger.debug('receive_message: creating request')
            self.receiveRequest = self.MPI.COMM_WORLD.irecv(self.default_message_buffer_size, self.MPI.ANY_SOURCE)
        # check status of current request
        if self.receiveRequest is not None:
            self.logger.debug('receive_message: checking request state')
            starttime = time.time()
            self.logger.debug('receive_message: enter block loop, block = %s, timeout = %s', block, timeout)
            while True:
                # self.logger.debug('check for MPI message')
                status = self.MPI.Status()
                # test to see if message was received
                self.logger.debug('receive_message: test request')
                message_received,message = self.receiveRequest.test(status=status)
                self.logger.debug('receive_message: done testing')
                # if received reset and return source rank and message content
                if message_received:
                    # self.logger.debug('MPI message received: %s',message)
                    self.receiveRequest = None
                    # add source rank to the message
                    message['source_rank'] = status.Get_source()
                    return message

                duration = time.time() - starttime
                if not block or duration > timeout:
                    self.logger.debug('receive_message: receive timed out exiting blocking MPI receive loop')
                    return None
                elif not self.queues['MPIService'].empty():
                    self.logger.debug('receive_message: message queue has input exiting blocking MPI receive loop')
                    return None
                else:
                    # self.logger.debug('sleep 1 second: block=%s timeout=%s time=%s',block,timeout,duration)
                    time.sleep(1)
        # no message received return nothing
        return None

    def forward_message(self, message):

        if message['type'] in self.forwarding_map:
            for thread_name in self.forwarding_map[message['type']]:
                self.logger.info('forwarding recieved MPI with type %s message to %s',message['type'],thread_name)
                self.queues[thread_name].put(message)
        else:
            self.logger.warning('received message type with no forwarding defined: %s',message['type'])

    '''
    def read_config(self):
       # get self.default_message_buffer_size
       if self.config.has_option(config_section,'default_message_buffer_size'):
          self.default_message_buffer_size = self.config.getint(config_section,'default_message_buffer_size')
       else:
          self.logger.error('must specify "default_message_buffer_size" in "%s" section of config file',config_section)
          return
       self.logger.info('default_message_buffer_size: %d',self.default_message_buffer_size)
 
       # get self.debug_message_char_length
       if self.config.has_option(config_section,'debug_message_char_length'):
          self.debug_message_char_length = self.config.getint(config_section,'debug_message_char_length')
       else:
          default = 100
          self.logger.warning('no "debug_message_char_length" in "%s" section of config file, using default %s',config_section,default)
          self.debug_message_char_length = default
       self.logger.info('debug_message_char_length: %d',self.debug_message_char_length)
 
 
       # read yoda log level:
       if self.config.has_option(config_section,'loglevel'):
          self.loglevel = self.config.get(config_section,'loglevel')
          self.logger.info('%s loglevel: %s',config_section,self.loglevel)
          self.logger.setLevel(logging.getLevelName(self.loglevel))
       else:
          self.logger.warning('no "loglevel" in "%s" section of config file, keeping default',config_section)
 
       # read yoda loop timeout:
       if self.config.has_option(config_section,'loop_timeout'):
          self.loop_timeout = self.config.getfloat(config_section,'loop_timeout')
       else:
          self.loop_timeout = 30
          self.logger.warning('no "loop_timeout" in "%s" section of config file, using default %s',config_section,self.loop_timeout)
       self.logger.info('%s loop_timeout: %d',config_section,self.loop_timeout)
 '''

# initialize the rank variables for other threads
# rank = MPI.COMM_WORLD.Get_rank()
# nranks = MPI.COMM_WORLD.Get_size()

# mpiService = MPIService()
# mpiService.start()
