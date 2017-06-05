import os,sys,logging,threading,subprocess,time,shutil
from mpi4py import MPI
from pandayoda.common import MessageTypes,SerialQueue,yoda_droid_messenger as ydm
logger = logging.getLogger(__name__)


config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]

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
      "PandaID":"3298217817",
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

   

   def __init__(self,config,queues,working_path):
      ''' 
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send 
                     messages to other Droid components about errors, etc.
        config: the ConfigParser handle for yoda
        working_path: the path where athena will execute
        '''
      # call base class init function
      super(JobManager,self).__init__()

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # ConfigParser handle for getting configuraiton information
      self.config                = config

      # working path for athenaMP
      self.working_path          = working_path

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
      ''' this is the main function run as a thread when the user calls jobManager_instance.start()'''
      if self.rank == 0:
         logger.debug('%s config_section: %s',self.prelog,config_section)

      # get loop_timeout
      if self.config.has_option(config_section,'loop_timeout'):
         loop_timeout = self.config.getfloat(config_section,'loop_timeout')
      else:
         logger.error('%s must specify "loop_timeout" in "%s" section of config file',self.prelog,config_section)
         return
      if self.rank == 0:
         logger.info('%s JobManager loop_timeout: %d',self.prelog,loop_timeout)

      # get droidSubprocessStdout
      if self.config.has_option(config_section,'droidSubprocessStdout'):
         self.droidSubprocessStdout = self.config.get(config_section,'droidSubprocessStdout')
      else:
         logger.error('%s must specify "droidSubprocessStdout" in "%s" section of config file',self.prelog,config_section)
         return
      if self.rank == 0:
         logger.info('%s JobManager droidSubprocessStdout: %s',self.prelog,self.droidSubprocessStdout)

      # get droidSubprocessStderr
      if self.config.has_option(config_section,'droidSubprocessStderr'):
         self.droidSubprocessStderr = self.config.get(config_section,'droidSubprocessStderr')
      else:
         logger.error('%s must specify "droidSubprocessStderr" in "%s" section of config file',self.prelog,config_section)
         return
      if self.rank == 0:
         logger.info('%s JobManager droidSubprocessStderr: %s',self.prelog,self.droidSubprocessStderr)

      waiting_for_panda_job = False
      # variable that holds currently running process (Popen object)
      jobproc = None

      job_request = None
      while not self.exit.isSet():
         logger.debug('%s start loop',self.prelog)
         # The JobManager should block on the most urgent task
         # during a fresh startup, it should block on receiving work
         # from other Droid elements, so it should message The YodaComm
         # for more work, then listen on the queue.
         # After receiving work, and starting the job subprocess, it
         # should block on the joining of that subprocess so that
         # as soon as it is completed, it moves to reqeust more work.


         # a job subprocess has been started previously
         if jobproc is not None:
            # check if job is still running
            procstatus = jobproc.poll()
            
            # subprocess is still running so wait for it to join
            if procstatus is None:
               logger.debug('%s wait for subprocess to join',self.prelog)
               time.sleep(loop_timeout)
            # subprocess completed
            else:
               returncode = procstatus
               logger.info('%s subprocess finished with return code = %i',
                           self.prelog,returncode)
               # set jobproc to None so a new job is retrieved
               jobproc = None
         # no subprocess started so we need to request a job.
         else:
            
            # if we haven't requested a job already, then do so
            if job_request is None:
               logger.debug('%s requesting new panda job',self.prelog)
               # send a request to Yoda to send another job
               job_request = ydm.send_job_request()
               # wait until this message is received
               job_request.wait()
               
               # can go ahead and request a reply from MPI
               job_request = ydm.recv_job()
            
             
            # test for requested job arrival
            flag,msg = job_request.test()
            # received a new job             
            if flag:  
               # check that the message type is correct
               if msg['type'] != MessageTypes.NEW_JOB:
                  logger.error('%s response from yoda does not have the expected type: %s',self.prelog,msg)
                  job_request = None
                  continue

               # check that there is a job
               if 'job' in msg:
                  logger.debug('%s received new job %s',self.prelog,msg['job'])

                  # send message to JobComm
                  self.queues['JobComm'].put(msg)

                  # copy input files to working_path
                  for file in msg['job']['inFiles'].split(','):
                     logger.debug('copying input file "%s" to working path %s',os.path.join(os.getcwd(),file),self.working_path)
                     shutil.copy(os.path.join(os.getcwd(),file),self.working_path)
                  
                  # start the new subprocess
                  jobproc = self.start_new_subprocess(msg['job'],self.working_path)
                  job_request = None

               else:
                  logger.error('%s no job found in yoda response: %s',self.prelog,msg)
                  job_request = None
            # did not receive job, so sleep
            else:
               logger.debug('%s no job yet received, sleeping %d ',self.prelog,loop_timeout)
               time.sleep(loop_timeout)
         
      if jobproc is not None:
         logger.info('%s killing job subprocess.',self.prelog)
         jobproc.kill()
      logger.info('%s JobManager thread exiting',self.prelog)

   def start_new_subprocess(self,new_job,working_path):
      ''' start the job subprocess, handling the command parsing and log file naming '''
      # parse filenames for the log output and check that they do not exist already
      stdoutfile = self.droidSubprocessStdout.format(rank='%03i'%self.rank,PandaID=new_job['PandaID'])
      stdoutfile = os.path.join(working_path,stdoutfile)
      if False: #os.path.exists(stdoutfile): # FIXME: for testing only, so remove later
         raise Exception('%s file already exists %s' % (self.prelog,stdoutfile))
      stderrfile = self.droidSubprocessStderr.format(rank='%03i'%self.rank,PandaID=new_job['PandaID'])
      stderrfile = os.path.join(working_path,stderrfile)
      if False: #os.path.exists(stderrfile): # FIXME: for testing only, so remove later
         raise Exception('%s file already exists %s' % (self.prelog,stderrfile))

      # parse the job into a command
      cmd = self.create_job_run_script(new_job,working_path)
      logger.debug('%s starting run_script: %s',self.prelog,cmd)
      
      jobproc = subprocess.Popen(['/bin/bash',cmd],stdout=open(stdoutfile,'w'),stderr=open(stderrfile,'w'),cwd=working_path)

      return jobproc

   def create_job_run_script(self,new_job,working_path):
      ''' using the template set in the configuration, create a script that 
          will run the panda job with the appropriate environment '''
      template_filename = self.config.get('JobManager','template')
      if os.path.exists(template_filename):
         template_file = open(template_filename)
         template = template_file.read()
         template_file.close()
         package,release = new_job['homepackage'].split('/')
         gcclocation = ''
         if release.startswith('19'):
            gcclocation  = '--gcclocation=$VO_ATLAS_SW_DIR/software/'
            gcclocation += '$CMTCONFIG/'
            gcclocation += '.'.join(release.split('.')[:-1])
            gcclocation += '/gcc-alt-472/$CMTCONFIG'
         #elif release.startswith('20'):
         
         script = template.format(transformation=new_job['transformation'],
                                  jobPars=new_job['jobPars'],
                                  cmtConfig=new_job['cmtConfig'],
                                  release=release,
                                  package=package,
                                  processingType=new_job['processingType'],
                                  pandaID=new_job['PandaID'],
                                  taskID=new_job['taskID'],
                                  gcclocation=gcclocation
                                 )

         script_filename = self.config.get('JobManager','run_script')
         script_filename = os.path.join(working_path,script_filename)
         script_file = open(script_filename,'w')
         script_file.write(script)
         script_file.close()

         return script_filename
      else:
         raise Exception('%s specified template does not exist: %s',self.prelog,template_filename)




# testing this thread
if __name__ == '__main__':
   logging.basicConfig(level=logging.DEBUG,
         format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
         datefmt='%Y-%m-%d %H:%M:%S')
   logging.info('Start test of JobManager')
   import time,argparse,ConfigParser
   oparser = argparse.ArgumentParser()
   oparser.add_argument('-y','--yoda-config', dest='yoda_config', help="The Yoda Config file where some configuration information is set.",default='yoda.cfg')
   oparser.add_argument('-w','--jobWorkingDir',dest='jobWorkingDir',help='Where to run the job',required=True)

   args = oparser.parse_args()


   os.chdir(args.jobWorkingDir)

   config = ConfigParser.ConfigParser()
   config.read(args.yoda_config)

   queues = {'JobComm':SerialQueue.SerialQueue(),'JobManager':SerialQueue.SerialQueue()}

   jm = JobManager(queues,config,5)

   jm.start()
   n = 0
   while True:
      logger.info('start loop')
      if not jm.isAlive(): break
      n+= 1

      logger.info('Yoda receiving job request from JobManager')
      status = MPI.Status()
      request = ydm.recv_job_request()
      msg = request.wait(status=status)
      logger.info('msg = %s',str(msg))

      if msg['type'] == MessageTypes.REQUEST_JOB:
         logger.info('Yoda received request for new job')
         job = {
      "PandaID": str(n), #str(int(time.time())), #"3298217817",
      "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
      #"PandaID": 3298217817,
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

      logger.info('Yoda sending new job message')
      ydm.send_droid_new_job(job,status.Get_source())



   jm.stop()

   jm.join()

   logger.info('exiting test')





