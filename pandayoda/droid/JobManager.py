import os,sys,logging,threading,subprocess,time,shutil
from mpi4py import MPI
from pandayoda.common import MessageTypes,SerialQueue,VariableWithLock
from pandayoda.common import yoda_droid_messenger as ydm
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

      # this is used to trigger the thread exit
      self.exit = threading.Event()

      # this flag is set when a message has been received that there are no more jobs to run
      self.no_more_jobs          = VariableWithLock.VariableWithLock(False)

   def stop(self):
      ''' this function can be called by outside threads to cause the JobManager thread to exit'''
      self.exit.set()

   def run(self):
      ''' this is the main function run as a thread when the user calls jobManager_instance.start()'''
      
      # read in configuration variables from config file
      self.read_config()

      # variable that holds currently running process (Popen object)
      jobproc = None
      
      # current job definition
      current_job = None

      # job request to monitor messaging
      job_request = None

      while not self.exit.isSet():
         logger.debug('start loop')
         # The JobManager should block on the most urgent task
         # during a fresh startup, it should block on receiving work
         # from other Droid elements, so it should message The YodaComm
         # for more work, then listen on the queue.
         # After receiving work, and starting the job subprocess, it
         # should block on the joining of that subprocess so that
         # as soon as it is completed, it moves to reqeust more work.

         

         # a job subprocess has been started previously
         if jobproc:
            
            # subprocess is still running so wait for it to join
            if jobproc.is_running():
               logger.debug('wait for subprocess to join')
               
               # check for message from droid before sleep
               try:
                  msg = self.queues['JobManager'].get(block=False)
                  logger.debug('received msg: %s',msg)
                  if msg['type'] == MessageTypes.WALLCLOCK_EXPIRING:
                     logger.debug('received wallclock expiring message, exiting loop')
                     # set exit flag to exit loop
                     self.stop()
                     break
                  else:
                     logger.warning('received unexpected message type')
               except SerialQueue.Empty:
                  logger.debug('no message on queue')

               time.sleep(self.loop_timeout)
            # subprocess completed
            else:
               returncode = jobproc.returncode()
               logger.info('subprocess finished with return code = %i',
                           self.prelog,returncode)
               # set jobproc to None so a new job is retrieved
               jobproc = None

            

         # no subprocess started so we need to request a job.
         else:
            
            logger.debug('requesting new panda job')

            # send message to MPIService to request job from Yoda
            msg = {
               'type':MessageTypes.REQUEST_JOB,
               'destination_rank': 0 # YODA rank
            }
            self.queues['MPIService'].put(msg)

            logger.debug('waiting for new panda job')
            # wait for response with new job
            msg = self.queues['JobManager'].get()

            # check that the message type is correct
            if msg['type'] == MessageTypes.NEW_JOB:
               
               # check that there is a job
               if 'job' in msg:
                  logger.debug('received new job %s',msg['job'])

                  # insert the Yampl AthenaMP setting
                  if 'jobPars' in msg['job']:
                     jobPars = msg['job']['jobPars']
                     if not "--preExec" in jobPars:
                        jobPars += " --preExec \'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\"\' " % self.yampl_socket_name
                     else:
                        if "import jobproperties as jps" in jobPars:
                           jobPars = jobPars.replace("import jobproperties as jps;", "import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\";" % self.yampl_socket_name)
                        else:
                           jobPars = jobPars.replace("--preExec ", "--preExec \'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\"\' " % self.yampl_socket_name)
                     msg['job']['jobPars'] = jobPars
                  else:
                     logger.error('erorr in job format')


                  logger.debug('running new job')

                  # send job definition to JobComm thread
                  self.queues['JobComm'].put(msg)

                  # copy input files to working_path
                  for file in msg['job']['inFiles'].split(','):
                     logger.debug('copying input file "%s" to working path %s',os.path.join(os.getcwd(),file),self.working_path)
                     shutil.copy(os.path.join(os.getcwd(),file),self.working_path)
                  
                  # start the new subprocess
                  jobproc = JobSubProcess(msg['job'],self.working_path,self.droidSubprocessStdout,self.droidSubprocessStderr)
                  jobproc.start()
                  
                  PANDA_JOB_REQUEST_SENT = False
               else:
                  logger.error('no job found in yoda response: %s',msg)
                  PANDA_JOB_REQUEST_SENT = False
            elif msg['type'] == MessageTypes.NO_MORE_JOBS:
               logger.info('received NO_MORE_JOBS, exiting.')
               self.no_more_jobs.set(True)
               self.stop()
            elif msg['type'] == MessageTypes.WALLCLOCK_EXPIRING:
               logger.info('received wallclock expiring message, exiting loop')
               # set exit flag to exit loop
               self.stop()
               break
            else:
               logger.error('received message but type %s unexpected',msg['type'])
            
         
      if jobproc is not None:
         logger.info('killing job subprocess.')
         jobproc.kill()
         while jobproc.is_running():
            logger.info('waiting for subprocess to exit.')
            time.sleep(2)
            jobproc.kill()

      logger.info('JobManager thread exiting')

   


   def read_config(self):
      logger.debug('config_section: %s',config_section)

      # get loop_timeout
      if self.config.has_option(config_section,'loop_timeout'):
         self.loop_timeout = self.config.getfloat(config_section,'loop_timeout')
      else:
         logger.error('must specify "loop_timeout" in "%s" section of config file',config_section)
         return
      if self.rank == 0:
         logger.info('JobManager loop_timeout: %d',self.loop_timeout)

      # get droidSubprocessStdout
      if self.config.has_option(config_section,'droidSubprocessStdout'):
         self.droidSubprocessStdout = self.config.get(config_section,'droidSubprocessStdout')
      else:
         logger.error('must specify "droidSubprocessStdout" in "%s" section of config file',config_section)
         return
      if self.rank == 0:
         logger.info('JobManager droidSubprocessStdout: %s',self.droidSubprocessStdout)

      # get droidSubprocessStderr
      if self.config.has_option(config_section,'droidSubprocessStderr'):
         self.droidSubprocessStderr = self.config.get(config_section,'droidSubprocessStderr')
      else:
         logger.error('must specify "droidSubprocessStderr" in "%s" section of config file',config_section)
         return
      if self.rank == 0:
         logger.info('JobManager droidSubprocessStderr: %s',self.droidSubprocessStderr)

      # get yampl_socket_name
      if self.config.has_option('Droid','yampl_socket_name'):
         self.yampl_socket_name = self.config.get('Droid','yampl_socket_name').format(rank_num='%03d' % self.rank)
      else:
         logger.error('must specify "yampl_socket_name" in "Droid" section of config file')
         return
      if self.rank == 0:
         logger.info('Droid yampl_socket_name: %s',self.yampl_socket_name)


      # get use_mock_athenamp
      if self.config.has_option(config_section,'use_mock_athenamp'):
         self.use_mock_athenamp = self.config.getboolean(config_section,'use_mock_athenamp')
      else:
         logger.error('must specify "use_mock_athenamp" in "%s" section of config file',config_section)
         return
      if self.rank == 0:
         logger.info('JobManager use_mock_athenamp: %s',self.use_mock_athenamp)

class JobSubProcess:
   def __init__(self,new_job,working_path,droidSubprocessStdout,droidSubprocessStderr):
      self.jobproc = None

      self.new_job = new_job
      self.working_path = working_path
      self.droidSubprocessStdout = droidSubprocessStdout
      self.droidSubprocessStderr = droidSubprocessStderr

   def kill(self):
      self.jobproc.kill()

   def is_running(self):
      # check if job is still running
      if jobproc.poll() is None: return True
      return False

   def returncode(self):
      return self.jobproc.returncode

   def start(self):
      ''' start the job subprocess, handling the command parsing and log file naming '''
      # parse filenames for the log output and check that they do not exist already
      stdoutfile = self.droidSubprocessStdout.format(rank='%03i'%self.rank,PandaID=new_job['PandaID'])
      stdoutfile = os.path.join(working_path,stdoutfile)
      if False: #os.path.exists(stdoutfile): # FIXME: for testing only, so remove later
         raise Exception('file already exists %s' % (self.prelog,stdoutfile))
      stderrfile = self.droidSubprocessStderr.format(rank='%03i'%self.rank,PandaID=new_job['PandaID'])
      stderrfile = os.path.join(working_path,stderrfile)
      if False: #os.path.exists(stderrfile): # FIXME: for testing only, so remove later
         raise Exception('file already exists %s' % (self.prelog,stderrfile))

      # parse the job into a command
      cmd = self.create_job_run_script()
      logger.debug('starting run_script: %s',cmd)
      
      self.jobproc = subprocess.Popen(['/bin/bash',cmd],stdout=open(stdoutfile,'w'),stderr=open(stderrfile,'w'),cwd=working_path)


   def create_job_run_script(self):
      ''' using the template set in the configuration, create a script that 
          will run the panda job with the appropriate environment '''
      template_filename = self.config.get('JobManager','template')
      if os.path.exists(template_filename):
         template_file = open(template_filename)
         template = template_file.read()
         template_file.close()
         package,release = self.new_job['homepackage'].split('/')
         gcclocation = ''
         if release.startswith('19'):
            gcclocation  = '--gcclocation=$VO_ATLAS_SW_DIR/software/'
            gcclocation += '$CMTCONFIG/'
            gcclocation += '.'.join(release.split('.')[:-1])
            gcclocation += '/gcc-alt-472/$CMTCONFIG'
         #elif release.startswith('20'):

         # set transformation command
         if self.use_mock_athenamp:
            import inspect
            import pandayoda.droid.mock_athenamp
            script = inspect.getfile(pandayoda.droid.mock_athenamp).replace('.pyc','.py')
            #logger.info('script = %s',script)
            transformation = script
         else:
            transformation = self.new_job['transformation']
         
         script = template.format(transformation=transformation,
                                  jobPars=self.new_job['jobPars'],
                                  cmtConfig=self.new_job['cmtConfig'],
                                  release=release,
                                  package=package,
                                  processingType=self.new_job['processingType'],
                                  pandaID=self.new_job['PandaID'],
                                  taskID=self.new_job['taskID'],
                                  gcclocation=gcclocation
                                 )

         script_filename = self.config.get('JobManager','run_script')
         script_filename = os.path.join(self.working_path,script_filename)
         script_file = open(script_filename,'w')
         script_file.write(script)
         script_file.close()

         return script_filename
      else:
         raise Exception('specified template does not exist: %s',template_filename)



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

   config.set('JobManager','use_mock_athenamp','true')

   queues = {'JobComm':SerialQueue.SerialQueue(),'JobManager':SerialQueue.SerialQueue()}

   # create droid working path
   droid_working_path = os.path.join(args.jobWorkingDir,'droid')
   if not os.path.exists(droid_working_path):
      os.makedirs(droid_working_path)

   jm = JobManager(config,queues,droid_working_path)
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

         if n > 1: # already sent a job, test exit
            ydm.send_droid_no_job_left(status.Get_source())
            break

         # create a dummy input file
         inputfilename = 'EVNT.06402143._000615.pool.root.1'
         if not os.path.exists(inputfilename):
            os.system('echo "dummy_data" > ' + inputfilename)
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





