# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)
# - Paul Nilsson (paul.nilsson@cern.ch)

import os
import logging
import time
import Queue
from pandayoda.common.yoda_multiprocessing import Process, Event
from pandayoda.common import MessageTypes
logger = logging.getLogger(__name__)

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


class FileManager(Process):
   IDLE = 'IDLE'
   STAGE_OUT = 'STAGE_OUT'
   STAGING_OUT = 'STAGING_OUT'
   EXITED = 'EXITED'

   STATES = [IDLE, STAGE_OUT, STAGING_OUT, EXITED]

   def __init__(self, config, queues, yoda_working_path, harvester_messenger):
      super(FileManager, self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # configuration of Yoda
      self.config                = config

      # this is used to trigger the thread exit
      self.exit                  = Event()

      # this is the working directory of yoda
      self.yoda_working_path     = yoda_working_path

      # harvester communication module
      self.harvester_messenger   = harvester_messenger

      # default harvester_output_timeout
      self.harvester_output_timeout = 10

   def stop(self):
      ''' this function can be called by outside threads to cause the JobManager thread to exit'''
      self.exit.set()


   def run(self):
      logger.debug('starting thread')

      # read configuration info from file
      self.read_config()

      local_filelist = []

      last_check = time.time()

      while not self.exit.is_set():
         logger.debug('starting loop, local_filelist size: %s',len(local_filelist))

         # process incoming messages
         try:
            qmsg = self.queues['FileManager'].get(timeout=self.loop_timeout)
         except Queue.Empty:
            logger.debug('queue is empty')
         else:

            logger.debug('message received: %s',qmsg)

            if qmsg['type'] == MessageTypes.OUTPUT_FILE:

               local_filelist += qmsg['filelist']
               logger.info('received output file, waiting list contains %s files',len(local_filelist))

               # I don't want to constantly check to see if the output file exists
               # so I'll only check every few seconds
               if time.time() - last_check > self.harvester_output_timeout:
                  last_check = time.time()

                  # if an output file already exists,
                  # wait for harvester to read in file, so add file list to
                  # local running list
                  if not self.harvester_messenger.stage_out_file_exists():
                     # add file to Harvester stage out
                     logger.info('staging %s files to Harvester',len(local_filelist))
                     self.harvester_messenger.stage_out_files(
                        local_filelist,
                        self.output_file_type
                     )
                     local_filelist = []
                  else:
                     logger.warning('Harvester has not yet consumed output files, currently waiting to dump %s output files',len(local_filelist))
            else:
               logger.error('message type not recognized')

      if local_filelist:
         logger.info('staging %s files to Harvester',len(local_filelist))
         self.harvester_messenger.stage_out_files(local_filelist, self.output_file_type)

      # exit
      logger.info('FileManager exiting')

   def read_config(self):

      if config_section in self.config:
         # read log level:
         if 'loglevel' in self.config[config_section]:
            self.loglevel = self.config[config_section]['loglevel']
            logger.info('%s loglevel: %s',config_section,self.loglevel)
            logger.setLevel(logging.getLevelName(self.loglevel))
         else:
            logger.warning('no "loglevel" in "%s" section of config file, keeping default',config_section)

         # read droid loop timeout:
         if 'loop_timeout' in self.config[config_section]:
            self.loop_timeout = int(self.config[config_section]['loop_timeout'])
            logger.info('%s loop_timeout: %s',config_section,self.loop_timeout)
         else:
            logger.warning('no "loop_timeout" in "%s" section of config file, keeping default %s',config_section,self.loop_timeout)


         # read harvester_output_timeout:
         if 'harvester_output_timeout' in self.config[config_section]:
            self.harvester_output_timeout = int(self.config[config_section]['harvester_output_timeout'])
            logger.info('%s harvester_output_timeout: %s',config_section,self.harvester_output_timeout)
         else:
            logger.warning('no "harvester_output_timeout" in "%s" section of config file, keeping default %s',config_section,self.harvester_output_timeout)

         # read output_file_type:
         if 'output_file_type' in self.config[config_section]:
            self.output_file_type = self.config[config_section]['output_file_type']
            logger.info('%s output_file_type: %s',config_section,self.output_file_type)
         else:
            logger.error('no "output_file_type" in "%s" section of config file, keeping default %s',config_section,self.output_file_type)
            raise Exception('must specify "output_file_type" in %s section of config file. Typically set to "es_output"' % config_section)

      else:
         raise Exception('no %s section in the configuration' % config_section)







