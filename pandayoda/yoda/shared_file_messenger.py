import os,json,logging,ConfigParser,time,threading,shutil
from mpi4py import MPI
from pandayoda.common import exceptions,serializer
logger = logging.getLogger(__name__)

''' 

This is a collection of functions used by Yoda/Droid to communicate with Harvester. It can be exchanged with other plugins 

The jobSpecFile format is like this:
=================================================================================
{
   "3298217817": {
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
}
=================================================================================

The eventRangesFile format looks like this:

=================================================================================
{
   "3298217817": [
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
   ],
}

=================================================================================


'''


# global to store the Harvester config
harvesterConfig = None
# acquire/release when accessing the harvester config, this avoids collisions.
# I put this in because the setup function was being called in parallel with the requestevents()
# this caused the request events to fail because it was moving faster than the setup function
# and therefore could not find the configuration information yet and threw an exception.
harConfLock = threading.Lock() 
harConfSect = 'payload_interaction'
request_polling_time = 5
request_poll_timeout = 300

def setup(config):
   global harvesterConfig,harConfLock
   harConfLock.acquire()
   if harvesterConfig is None:
      logger.debug('Rank %05i: loading harvester configuration file',MPI.COMM_WORLD.Get_rank())
      # get harvester config filename
      harv_config_file = config.get('shared_file_messenger','harvester_config_file')

      # parse harvester configuration file
      if os.path.exists(harv_config_file):
         harvesterConfig = ConfigParser.ConfigParser()
         harvesterConfig.read(harv_config_file)
      else:
         harConfLock.release()
         raise Exception('Rank %05i: Failed to parse config file: %s' % (harv_config_file,MPI.COMM_WORLD.Get_rank()))
   else:
      logger.debug('Rank %05i: harvester configuration already loaded',MPI.COMM_WORLD.Get_rank())

   harConfLock.release()

def requestjobs():
   global harvesterConfig,harConfSect,harConfLock
   
   if harvesterConfig is None:
      logger.error('Rank %05i: must first run setup before requestjobs',MPI.COMM_WORLD.Get_rank())
      return
   try:
      harConfLock.acquire()
      jobRequestFile = harvesterConfig.get(harConfSect,'jobRequestFile')
      harConfLock.release()
   except ConfigParser.NoSectionError:
      harConfLock.release()
      raise exceptions.MessengerConfigError('Rank %05i: could not find section "%s" in configuration for harvester, available sections are: %s' % (MPI.COMM_WORLD.Get_rank(),harConfSect,harvesterConfig.sections()))

   if not os.path.exists(jobRequestFile):
      open(jobRequestFile,'w').write('jobRequestFile')
   else:
      raise exceptions.MessengerJobAlreadyRequested


# this function should return a job description or nothing
def get_pandajobs():
   global harvesterConfig,harConfSect,request_polling_time,request_poll_timeout,harConfLock
   
   if harvesterConfig is None:
      logger.error('Rank %05i: must first run setup before get_pandajobs',MPI.COMM_WORLD.Get_rank())
      return

   # the file in which job descriptions would be stored
   try:
      harConfLock.acquire()
      jobSpecFile = harvesterConfig.get(harConfSect,'jobSpecFile')
      harConfLock.release()
   except ConfigParser.NoSectionError:
      harConfLock.release()
      raise exceptions.MessengerConfigError('Rank %05i: could not find section "%s" in configuration for harvester, available sections are: %s' % (MPI.COMM_WORLD.Get_rank(),harConfSect,harvesterConfig.sections()))

   # first check to see if a file already exists.
   if os.path.exists(jobSpecFile):
      try:
         logger.debug('jobSpecFile is present, reading job definitions')
         # parse job spec file
         job_def = json.load(open(jobSpecFile))
         # remove this file now that we are done with it
         os.remove(jobSpecFile)
         # remove request file if harvester has not already
         if os.path.exists(harvesterConfig.get(harConfSect,'jobRequestFile')):
            os.remove(harvesterConfig.get(harConfSect,'jobRequestFile'))
         # return job definition
         return job_def
      except:
         logger.exception('Rank %05i: failed to parse jobSpecFile: %s' % (MPI.COMM_WORLD.Get_rank(),jobSpecFile))
         raise
   # if the file does not exist, request one
   else:
      try:
         logger.debug('jobSpecFile is absent, requesting job definitions')
         requestjobs()
      except exceptions.MessengerJobAlreadyRequested:
         logger.debug('Rank %05i: duplicate job request',MPI.COMM_WORLD.Get_rank())
         raise

      
      # now wait for the file to show up
      i = int(request_poll_timeout*1./request_polling_time)
      while not os.path.exists(jobSpecFile) and i:
         logger.debug('Rank %05i: waiting for jobSpecFile to appear, try %d',MPI.COMM_WORLD.Get_rank(),i)
         time.sleep(request_polling_time)
         i -= 1

      # if the loop timed out, assume there are no more events left
      if i == 0:
         return {}
      
      # now parse job file
      try:
         logger.debug('jobSpecFile has arrived, parsing job definitions')
         # parse job spec file
         job_def = json.load(open(jobSpecFile))
         # remove this file now that we are done with it
         os.remove(jobSpecFile)
         # remove request file if harvester has not already
         if os.path.exists(harvesterConfig.get(harConfSect,'jobRequestFile')):
            os.remove(harvesterConfig.get(harConfSect,'jobRequestFile'))
         # return job definition
         return job_def
      except:
         logger.exception('Rank %05i: failed to parse jobSpecFile: %s' % (MPI.COMM_WORLD.Get_rank(),jobSpecFile))
         raise


def requesteventranges(job_def):
   global harvesterConfig,harConfSect,harConfLock
   
   if harvesterConfig is None:
      logger.error('Rank %05i: must first run setup before requesteventranges',MPI.COMM_WORLD.Get_rank())
      return

   try:
      harConfLock.acquire()
      eventRequestFile = harvesterConfig.get(harConfSect,'eventRequestFile')
      harConfLock.release()
      # using a temp file name then moving to avoid timiing issues
      eventRequestFile_tmp = eventRequestFile + '.tmp'
   except ConfigParser.NoSectionError:
      harConfLock.release()
      raise exceptions.MessengerConfigError('Rank %05i: could not find section "%s" in configuration for harvester, available sections are: %s' % (MPI.COMM_WORLD.Get_rank(),harConfSect,harvesterConfig.sections()))

   if not os.path.exists(eventRequestFile):
      # need to output a file containing:
      #   {'nRanges': ???, 'pandaID':???, 'taskID':???, 'jobsetID':???}
      logger.debug('Rank %05i: requesting new event ranges with job_def = %s',MPI.COMM_WORLD.Get_rank(),job_def)
      f = open(eventRequestFile_tmp,'w')
      f.write(serializer.serialize(job_def)) 
      f.close()

      # now move tmp filename to real filename
      os.rename(eventRequestFile_tmp,eventRequestFile)

   else:
      raise exceptions.MessengerEventRangesAlreadyRequested

def get_eventranges(job_def):
   global harvesterConfig,harConfSect,request_polling_time,request_poll_timeout,harConfLock
   
   # check that harvester config is loaded
   if harvesterConfig is None:
      logger.error('Rank %05i: must first run setup before get_eventranges',MPI.COMM_WORLD.Get_rank())
      return

   # load name of events file
   try:
      harConfLock.acquire()
      eventRangesFile = harvesterConfig.get(harConfSect,'eventRangesFile')
      harConfLock.release()
   except ConfigParser.NoSectionError:
      harConfLock.release()
      raise exceptions.MessengerConfigError('Rank %05i: could not find section "%s" in configuration for harvester, available sections are: %s' % (MPI.COMM_WORLD.Get_rank(),harConfSect,harvesterConfig.sections()))

   # first check to see if a file already exists.
   if os.path.exists(eventRangesFile):
      try:
         logger.debug('eventRangesFile is present, parsing event ranges')
         # read in event range file
         eventranges = json.load(open(eventRangesFile))
         # remove this file now that we are done with it
         os.remove(eventRangesFile)
         # remove the request file if harvester has not already
         if os.path.exists(harvesterConfig.get(harConfSect,'eventRangesFile')):
            os.remove(harvesterConfig.get(harConfSect,'eventRangesFile'))
         # return event ranges
         return eventranges
      except:
         logger.exception('Rank %05i: failed to parse eventRangesFile: %s',eventRangesFile)
         raise
   # if the file does not exist, request one
   else:
      try:
         requesteventranges(job_def)
      except exceptions.MessengerEventRangesAlreadyRequested:
         logger.debug('Rank %05i: duplicate job request',MPI.COMM_WORLD.Get_rank())
         raise

      # now wait for the file to show up
      i = int(request_poll_timeout*1./request_polling_time)
      while not os.path.exists(eventRangesFile) and i:
         logger.debug('Rank %05i: waiting for eventRangesFile to appear, try %d',MPI.COMM_WORLD.Get_rank(),i)
         time.sleep(request_polling_time)
         i -= 1
      
      # if the loop timed out, assume there are no more events left
      if i == 0:
         return {}

      # parse file
      try:
         logger.debug('eventRangesFile is present, parsing event ranges')
         # read in event range file
         eventranges = json.load(open(eventRangesFile))
         # remove this file now that we are done with it
         os.remove(eventRangesFile)
         # remove the request file if harvester has not already
         if os.path.exists(harvesterConfig.get(harConfSect,'eventRangesFile')):
            os.remove(harvesterConfig.get(harConfSect,'eventRangesFile'))
         # return event ranges
         return eventranges
      except:
         logger.exception('Rank %05i: failed to parse eventRangesFile: %s' % (eventRangesFile,MPI.COMM_WORLD.Get_rank()))
         raise


'''
The worker needs to put eventStatusDumpJsonFile to update events and/or stage-out. The file is a json dump of a dictionary {pandaID_1:[{'eventRangeID':???, 'eventStatus':???, 'path':???, 'type':???, 'chksum':???, 'guid':???}, ...], pandaID_2:[...], ...}. Only 'eventRangeID' and 'eventStatus' are required to update events, while other file information such as 'path', 'type' and 'chksum' are required in addition if the event produced an output file. 'type' is the type of the file and it can be 'output', 'es_output', or 'log'. 'eventRangeID' should be removed unless the type is 'es_output'. Harvester uploads files depending on the 'type'. 'chksum' is calculated using adler32 if omitted. If the output has an intrinsic guid (this is the case for POOL files) 'guid' needs to be set. eventStatusDumpJsonFile is deleted once Harvester updates the database.
'''

def stage_out_file(output_type,output_path,eventRangeID,eventStatus,pandaID,chksum=None,):
   global harvesterConfig,harConfSect,request_polling_time,request_poll_timeout,harConfLock

   if output_type not in ['output','es_output','log']:
      raise Exception('Rank %05i: incorrect type provided: %s' % (MPI.COMM_WORLD.Get_rank(),output_type))

   if not os.path.exists(output_path):
      raise Exception('Rank %05i: output file not found: %s' % (MPI.COMM_WORLD.Get_rank(),output_path))
   
   # check that harvester config is loaded
   if harvesterConfig is None:
      raise Exception('Rank %05i: must first run setup before get_eventranges' % MPI.COMM_WORLD.Get_rank())

   # make sure pandaID is a string
   pandaID = str(pandaID)
      

   # load name of eventStatusDumpJsonFile file
   try:
      harConfLock.acquire()
      eventStatusDumpJsonFile = harvesterConfig.get(harConfSect,'eventStatusDumpJsonFile')
      harConfLock.release()
   except ConfigParser.NoSectionError:
      harConfLock.release()
      raise exceptions.MessengerConfigError('Rank %05i: could not find section "%s" in configuration for harvester, available sections are: %s' % (MPI.COMM_WORLD.Get_rank(),harConfSect,harvesterConfig.sections()))

   # first create a temp file to place contents
   # this avoids Harvester trying to read the file while it is being written
   eventStatusDumpJsonFile_tmp = eventStatusDumpJsonFile + '.tmp'
   

   # format data for file:
   file_descriptor = {'eventRangeID':eventRangeID,
                      'eventStatus':eventStatus,
                      'path':output_path,
                      'type:':output_type,
                      'chksum': chksum,
                      'guid': None,
                     }

   # if file does not already exists, new data is just what we have
   if not os.path.exists(eventStatusDumpJsonFile):
      data = {pandaID: [file_descriptor]}
   
   # if the file exists, move it to a tmp filename, update its contents and then recreate it.
   else:

      # first move existing file to tmp so Harvester does not read it while we edit
      os.rename(eventStatusDumpJsonFile,eventStatusDumpJsonFile_tmp)

      # now open and read in the data
      with open(eventStatusDumpJsonFile_tmp,'r') as f:
         data = serializer.deserialize(f.read())
      logger.debug('existing data contains %s',data)
      # if the pandaID already exists, just append the new file to that list
      if pandaID in data:
         logger.debug('addding data to existing panda list')
         data[pandaID].append(file_descriptor)
      # if the pandaID does not exist, add a new list
      else:
         logger.debug('addding new panda id list')
         data[pandaID] = [file_descriptor]

   logger.debug('output to file %s: %s',eventStatusDumpJsonFile,data)
   
   # overwrite the temp file with the updated data
   with open(eventStatusDumpJsonFile_tmp,'w') as f:
      f.write(serializer.serialize(data))

   # move tmp file into place
   os.rename(eventStatusDumpJsonFile_tmp,eventStatusDumpJsonFile)

   







