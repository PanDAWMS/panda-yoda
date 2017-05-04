import os,sys,logging,threading,json,subprocess,SerialQueue
from mpi4py import MPI
import MessageTypes
logger = logging.getLogger(__name__)

class JobManager(threading.Thread):
   ''' JobManager: This Thread will launch the ATLAS Transform and monitor it 
                   until it exits. Error handling, recovery, and restart operations 
                   should also be handled here. 

      The input/output message format will be in the form of json.
      The the message should be a dictionary of this form:
         {'type': MSG_TYPES,...} where MSG_TYPES should be replaces by one of the members of that list
         for SEND_NEW_PANDA_JOB messages:
            {'type': SEND_NEW_PANDA_JOB}
         for NO_MORE_WORK messages:
            {'type': NO_MORE_WORK}
         for NEW_PANDA_JOB messages:
            {'type': NEW_PANDA_JOB,
             'job': {
      "jobid":"3298217817",
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

   '''

   

   def __init__(self,queues,
                loopTimeout            = 30,
                droidSubprocessStdout  = 'droid_job_r{rank}_jid{jobid}.stdout',
                droidSubprocessStderr  = 'droid_job_r{rank}_jid{jobid}.stderr',
                testing                = False):
      ''' 
         queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send 
                     messages to other Droid components about errors, etc.
         loopTimeout: A positive number of seconds to wait between loop execution.
         droidSubprocessStdout: pipe subprocess stdout to this filename, this can 
                     use python string format keys 'rank' and 'jobid' to insert the 
                     Droid rank number and the current panda job id.
         droidSubprocessStderr: pipe subprocess stderr to this filename, this can 
                     use python string format keys 'rank' and 'jobid' to insert the 
                     Droid rank number and the current panda job id.'''
      # call base class init function
      super(JobManager,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # the timeout duration when attempting to retrieve an input message
      self.loopTimeout           = loopTimeout

      # get current rank
      self.rank                  = MPI.COMM_WORLD.Get_rank()

      # the prelog is just a string to attach before each log message
      self.prelog                = 'Rank %03i:' % self.rank

      # this flag keeps track of whether or not the request for more work has
      # been sent, to avoid sending it more than once
      self.waiting_for_panda_job = False

      # subprocess STDOUT/STDERR will be piped to these files
      self.droidSubprocessStdout = droidSubprocessStdout
      self.droidSubprocessStderr = droidSubprocessStderr

      # this is used to trigger the thread exit
      self.exit = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the JobManager thread to exit'''
      self.exit.set()


   def run(self):
      ''' this is the main function run as a thread when the user calls jobManager_instance.start()'''

      jobproc = None
      while not self.exit.isSet():
         logger.debug('%s start loop',self.prelog)
         # The JobManager should block on the most urgent task
         # during a fresh startup, it should block on receiving work
         # from other Droid elements, so it should message The YodaComm
         # for more work, then listen on the queue.
         # After receiving work, and starting the job subprocess, it
         # should block on the joining of that subprocess so that
         # as soon as it is completed, it moves to reqeust more work.


         if jobproc is not None:
            # if jobproc is not None, a job subprocess has been started previously
            # check if job is still running
            procstatus = jobproc.poll()
            if procstatus is None:
               # subprocess is still running so wait for it to join
               logger.debug('%s wait for subprocess to join',self.prelog)
               time.sleep(self.loopTimeout)
            else:
               # subprocess completed
               returncode = procstatus
               logger.info('%s subprocess finished with return code = %i',
                           self.prelog,returncode)
               # set jobproc to None so a new job is retrieved
               jobproc = None
         else:
            # jobproc is None so we need to request a job.

            if not self.waiting_for_panda_job:
               # if we haven't requested a job already, then do so
               logger.debug('%s request new panda job',self.prelog)
               self.request_new_panda_job()
            else:
               # wait for requested job to arrive
               try:
                  # this retrieve acts as the loop execution frequency control
                  logger.debug('%s get message',self.prelog)
                  message = self.queues['JobManager'].get(timeout=self.loopTimeout)
               except SerialQueue.Empty:
                  # no messages on the queue, check status of running job
                  logger.debug('%s no message received',self.prelog)
               else:
                  
                  if message['type'] == MessageTypes.NEW_PANDA_JOB:
                     # received new panda job
                     new_job = message['job']
                     # set waiting flag to False since we received our new job
                     self.waiting_for_panda_job = False

                     # start the new subprocess
                     logger.debug('%s starting new subprocess',self.prelog)
                     jobproc = self.start_new_subprocess(new_job)
         

      logger.info('%s JobManager thread exiting',self.prelog)


   def request_new_panda_job(self):
      ''' send request to droid thread for more work, but check that a message has not already been sent '''
      # do not send new requests if one has already been sent
      if not self.waiting_for_panda_job:
         message = {'type':MessageTypes.SEND_NEW_PANDA_JOB}
         try:
            self.queues['YodaComm'].put(message,timeout=30)
            self.waiting_for_panda_job = True
         except SerialQueue.Full:
            logger.warning('%s YodaComm queue is full',self.prelog)
      else:
         logger.debug('%s waiting for new panda job',self.prelog)

   def start_new_subprocess(self,new_job):
      ''' start the job subprocess, handling the command parsing and log file naming '''
      # parse filenames for the log output and check that they do not exist already
      stdoutfile = self.droidSubprocessStdout.format(rank='%03i'%self.rank,jobid=new_job['jobid'])
      if os.path.exists(stdoutfile):
         raise Exception('%s file already exists %s' % (self.prelog,stdoutfile))
      stderrfile = self.droidSubprocessStderr.format(rank='%03i'%self.rank,jobid=new_job['jobid'])
      if os.path.exists(stderrfile):
         raise Exception('%s file already exists %s' % (self.prelog,stderrfile))

      # parse the job into a command
      cmd = self.parse_job_command(new_job)
      logger.debug('%s starting cmd: %s',self.prelog,cmd)
      
      jobproc = subprocess.Popen(cmd,shell=True,stdout=open(stdoutfile,'w'),stderr=open(stderrfile,'w'))

      return jobproc

   def parse_job_command(self,new_job):
      ''' parse the job description into a command for the subprocess '''
      cmd = ''
      cmd += new_job['transformation']
      cmd += ' ' + new_job['jobPars']
      return cmd


# testing this thread
if __name__ == '__main__':
   logging.basicConfig(level=logging.DEBUG,
         format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
         datefmt='%Y-%m-%d %H:%M:%S')
   logging.info('Start test of JobManager')
   import time
   #import argparse
   #oparser = argparse.ArgumentParser()
   #oparser.add_argument('-l','--jobWorkingDir', dest="jobWorkingDir", default=None, help="Job's working directory.",required=True)
   #args = oparser.parse_args()

   queues = {'YodaComm':SerialQueue.SerialQueue(),'JobManager':SerialQueue.SerialQueue()}

   jm = JobManager(queues,5)

   jm.start()
   n = 0
   while True:
      logger.info('start loop')
      if not jm.isAlive(): break
      n+= 1

      logger.info('get message')
      try:
         msg = queues['YodaComm'].get(timeout=5)
      except SerialQueue.Empty:
         logger.info('queue empty')
         continue
      logger.info('msg = %s',str(msg))

      if msg['type'] == MessageTypes.SEND_NEW_PANDA_JOB:
         logger.info('got message to send new job')
         reply = {
            'type': MessageTypes.NEW_PANDA_JOB,
            'job' :
               {
      "jobid": str(n), #str(int(time.time())), #"3298217817",
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

      logger.info('send message')
      queues['JobManager'].put(reply)



   jm.stop()

   jm.join()

   logger.info('exiting test')





