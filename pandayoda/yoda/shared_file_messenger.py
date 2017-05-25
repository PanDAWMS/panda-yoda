import os,json,logging,ConfigParser,time
from pandayoda.common import exceptions
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
harConfSect = 'payload_interaction'
request_polling_time = 5
request_poll_timeout = 360

def setup(config):
   global harvesterConfig

   if harvesterConfig is None:
      logger.debug('loading harvester configuration file')
      # get harvester config filename
      harv_config_file = config.get(__name__,'harvester_config_file')

      # parse harvester configuration file
      if os.path.exists(harv_config_file):
         harvesterConfig = ConfigParser.ConfigParser()
         harvesterConfig.read(harv_config_file)
      else:
         raise Exception('Failed to parse config file: %s' % harv_config_file)
   else:
      logger.debug('harvester configuration already loaded')


def requestjobs():
   global harvesterConfig,harConfSect
   
   if harvesterConfig is None:
      logger.error('must first run setup before requestjobs')
      return

   jobRequestFile = harvesterConfig.get(harConfSect,'jobRequestFile')
   if not os.path.exists(jobRequestFile):
      open(jobRequestFile,'w').write('jobRequestFile')
   else:
      raise exceptions.MessengerJobAlreadyRequested()


# this function should return a job description or nothing
def get_pandajobs():
   global harvesterConfig,harConfSect,request_polling_time,request_poll_timeout
   
   if harvesterConfig is None:
      logger.error('must first run setup before get_pandajobs')
      return

   # the file in which job descriptions would be stored
   jobSpecFile = harvesterConfig.get(harConfSect,'jobSpecFile')
   # first check to see if a file already exists.
   if os.path.exists(jobSpecFile):
      try:
         return json.load(open(jobSpecFile))
      except:
         logger.exception('failed to parse jobSpecFile: %s',jobSpecFile)
         raise
   # if the file does not exist, request one
   else:
      requestjobs()
      # now wait for the file to show up
      i = int(request_poll_timeout*1./request_polling_time)
      while not os.path.exists(jobSpecFile) and i:
         time.sleep(request_polling_time)
         i -= 1

      # if the loop timed out, assume there are no more events left
      if i == 0:
         return {}
      
      # now parse job file
      try:
         return json.load(open(jobSpecFile))
      except:
         logger.exception('failed to parse jobSpecFile: %s',jobSpecFile)
         raise


def requesteventranges():
   global harvesterConfig,harConfSect
   
   if harvesterConfig is None:
      logger.error('must first run setup before requesteventranges')
      return

   eventRequestFile = harvesterConfig.get(harConfSect,'eventRequestFile')
   if not os.path.exists(eventRequestFile):
      open(eventRequestFile,'w').write('eventRequestFile') 
   else:
      raise exceptions.MessengerEventRangesAlreadyRequested()

def get_eventranges():
   global harvesterConfig,harConfSect,request_polling_time,request_poll_timeout
   if harvesterConfig is None:
      logger.error('must first run setup before get_eventranges')
      return

   eventRangesFile = harvesterConfig.get(harConfSect,'eventRangesFile')
   # first check to see if a file already exists.
   if os.path.exists(eventRangesFile):
      try:
         return json.load(open(eventRangesFile))
      except:
         logger.exception('failed to parse eventRangesFile: %s',eventRangesFile)
         raise
   # if the file does not exist, request one
   else:
      requesteventranges()
      # now wait for the file to show up
      i = int(request_poll_timeout*1./request_polling_time)
      while not os.path.exists(eventRangesFile) and i:
         time.sleep(request_polling_time)
         i -= 1
      
      # if the loop timed out, assume there are no more events left
      if i == 0:
         return {}

      # parse file
      try:
         return json.load(open(eventRangesFile))
      except:
         logger.exception('failed to parse eventRangesFile: %s',eventRangesFile)
         raise





