import os,logging,subprocess,random,glob,shutil
from pandayoda.common import VariableWithLock,MPIService,StatefulService
logger = logging.getLogger(__name__)

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


class TransformManager(StatefulService.StatefulService):
   ''' This service is handed a job definition and when the parent process
       calls the start() function, it launches the transform defined by the
       panda job.'''

   CREATED     = 'CREATED'
   STARTED     = 'STARTED'
   MONITORING  = 'MONITORING'
   FINISHED    = 'FINISHED'
   STATES      = [CREATED,STARTED,MONITORING,FINISHED]

   def __init__(self,job_def,
                     config,
                     queues,
                     droid_working_path,
                     yoda_working_path,
                     yampl_socket_name):
      '''
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send
                     messages to other Droid components about errors, etc.
        config: the ConfigParser handle for yoda
        droid_working_path: the path where athena will execute
        '''
      # call base class init function
      super(TransformManager,self).__init__()

      # yoda config file
      self.config                = config

      # job definition from panda
      self.job_def               = job_def

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # working path for athenaMP
      self.droid_working_path    = droid_working_path

      # working path for Yoda (where input files are located)
      self.yoda_working_path     = yoda_working_path

      # socket name to pass to transform for use when communicating via yampl
      self.yampl_socket_name     = yampl_socket_name

      # set default runscript filename
      self.runscript_filename    = 'runscript.sh'

      # set default use_container
      self.use_container         = False

      # set default run_elsewhere
      self.run_elsewhere         = False

      # set default logs_to_stage
      self.logs_to_stage         = []

      # return code, set only after exit
      self.returncode            = VariableWithLock.VariableWithLock()

      # set state to initial
      self.set_state(TransformManager.CREATED)

   def get_returncode(self):
      self.returncode.get()

   def run(self):
      ''' start and monitor transform in subprocess '''

      # read config
      self.read_config()
      

      self.set_state(TransformManager.STARTED)

      # first step, create subprocess
      logger.info('create and start subprocess for transform')
      self.start_subprocess()

      # third step, loop to monitor process
      logger.info('monitoring subprocess for transform')
      self.set_state(TransformManager.MONITORING)
      while not self.exit.wait(timeout=self.loop_timeout):
         logger.debug('start moinitor loop, timeout = %s',self.loop_timeout)

         if not self.is_subprocess_running():
            logger.info('transform has finished')
            break
      
      self.set_state(TransformManager.FINISHED)

      # incase we are exiting because parent process told us to exit
      # we should kill the subprocess
      if self.exit.isSet() and self.is_subprocess_running():
         logger.info('signaled to exit before subprocess exited so killing it.')
         self.jobproc.kill()

      self.returncode.set(self.subprocess_returncode())

      if self.run_elsewhere:
         self.stage_logs()

      logger.info('exiting')

   def create_job_run_script(self):
      ''' using the template set in the configuration, create a script that
          will run the panda job with the appropriate environment '''
      logger.debug('JobSubProcess: create job run script')
      template_filename = self.runscript_template
      
      template_file = open(template_filename)
      template = template_file.read()
      template_file.close()
      package,release = self.job_def['homepackage'].split('/')
      gcclocation = ''
      if release.startswith('19'):
         gcclocation  = '--gcclocation=$VO_ATLAS_SW_DIR/software/'
         gcclocation += '$CMTCONFIG/'
         gcclocation += '.'.join(release.split('.')[:-1])
         gcclocation += '/gcc-alt-472/$CMTCONFIG'

      # set transformation command
      transformation = self.job_def['transformation']
      logger.debug('got transformation: %s',transformation)
   

      # replace input file with full path
      input_files = self.job_def['inFiles'].split(',')
      updated_input_files = []
      for input_file in input_files:
         updated_input_files.append(os.path.join(self.yoda_working_path,input_file))
      
      # too long for jumbo jobs
      # logger.debug('inFiles: %s',input_files)
      # logger.debug('new files: %s',updated_input_files)

      start_index = self.job_def['jobPars'].find('--inputEVNTFile=') + len('--inputEVNTFile=')
      end_index   = self.job_def['jobPars'].find(' ',start_index)
      
      file_index = random.randint(0,len(updated_input_files) - 1)

      jobPars = self.job_def['jobPars'][:start_index] + updated_input_files[file_index] + ' ' + self.job_def['jobPars'][end_index:] + ' --fileValidation=FALSE'

      # insert the Yampl AthenaMP setting
      if "--preExec" not in jobPars:
         jobPars += " --preExec \'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\"\' " % self.yampl_socket_name
      else:
         if "import jobproperties as jps" in jobPars:
            jobPars = jobPars.replace("import jobproperties as jps;", "import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\";" % self.yampl_socket_name)
         else:
            jobPars = jobPars.replace("--preExec ", "--preExec \'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\"\' " % self.yampl_socket_name)
      
      if '--postExec' not in jobPars:
         jobPars += " --postExec 'svcMgr.MessageSvc.defaultLimit = 9999999;' "
      else:
         jobPars = jobPars.replace('--postExec ','--postExec "svcMgr.MessageSvc.defaultLimit = 9999999;" ')

      # these prints are too long with jumbo jobs
      # logger.debug('jobPars: %s',self.job_def['jobPars'])
      # logger.debug('new jobPars: %s',jobPars)

      # change working dir if need be
      working_dir = self.droid_working_path
      if self.run_elsewhere:
         working_dir = self.run_directory

      
      script = template.format(transformation=transformation,
                               jobPars=jobPars,
                               cmtConfig=self.job_def['cmtConfig'],
                               release=release,
                               package=package,
                               processingType=self.job_def['processingType'],
                               pandaID=self.job_def['PandaID'],
                               taskID=self.job_def['taskID'],
                               gcclocation=gcclocation,
                               working_dir=working_dir,
                              )

      script_filename = self.runscript_filename
      script_filename = os.path.join(self.droid_working_path,script_filename)
      script_file = open(script_filename,'w')
      script_file.write(script)
      script_file.close()

      logger.debug('script complete')
      return script_filename

      # else:
      #    raise Exception('specified template does not exist: %s',template_filename)

   def start_subprocess(self):
      ''' start the job subprocess, handling the command parsing and log file naming '''
      
      logger.debug('start JobSubProcess')

      # parse the job into a command
      script_name = self.create_job_run_script()

      # if container job add container prefix command
      if self.use_container:
         cmd = self.container_prefix + ' ' + script_name
         env = {}
      else:
         cmd = '/bin/bash ' + script_name
         env = {}

      # if job is to run elsewhere:
      cwd = self.droid_working_path
      if self.run_elsewhere:
         cwd = self.run_directory

      logger.info('running directory: %s',cwd)


      logger.debug('starting run_script: %s',cmd)
      
      self.jobproc = subprocess.Popen(cmd.split(),
                                      stdout=open(self.stdout_filename,'w'),
                                      stderr=open(self.stderr_filename,'w'),
                                      cwd=cwd,
                                      env=env,
                                     )

      logger.debug('transform is running')

   def kill_subprocess(self):
      if self.jobproc:
         self.jobproc.kill()
      else:
         raise Exception('tried killing subprocess, but subprocess is empty')

   def is_subprocess_running(self):
      if self.jobproc is None:
         return False
      # check if job is still running
      if self.jobproc.poll() is None:
         return True
      return False

   def subprocess_returncode(self):
      if self.jobproc is None:
         raise Exception('tried to get return code, but subprocess is empty')
      return self.jobproc.returncode


   def stage_logs(self):
      start_dir = os.getcwd()
      os.chdir(self.run_directory)
      for file in os.listdir(os.getcwd()):
         logger.info('files: %s',file)
      logs = []
      for entry in self.logs_to_stage:
         logs += glob.glob(entry)
      logger.info('staging %s files from %s to %s',len(logs),self.run_directory,self.droid_working_path)

      for filename in logs:
         cmd = 'cp --parents ' + filename + ' ' + self.droid_working_path + '/'
         logger.debug('copying: %s',cmd)
         os.system(cmd)

      os.chdir(start_dir)
      logger.debug('staging files completed')

   

   def read_config(self):
      # read log level:
      if self.config.has_option(config_section,'loglevel'):
         self.loglevel = self.config.get(config_section,'loglevel')
         logger.info('%s loglevel: %s',config_section,self.loglevel)
         logger.setLevel(logging.getLevelName(self.loglevel))
      else:
         logger.warning('no "loglevel" in "%s" section of config file, keeping default',config_section)

       # read droid loop timeout:
      if self.config.has_option(config_section,'loop_timeout'):
         self.loop_timeout = self.config.getfloat(config_section,'loop_timeout')
         logger.info('%s loop_timeout: %d',config_section,self.loop_timeout)
      else:
         logger.warning('no "loop_timeout" in "%s" section of config file, using default %s',config_section,self.loop_timeout)

      # read runscript template
      if self.config.has_option(config_section,'template'):
         self.runscript_template = self.config.get(config_section,'template')
      else:
         raise Exception('must specify "template" in "%s" section of config file' % config_section)
      logger.info('template: %s',self.runscript_template)

      # read runscript filename
      if self.config.has_option(config_section,'run_script'):
         self.runscript_filename = self.config.get(config_section,'run_script')
         logger.info('run_script: %s',self.runscript_filename)
      else:
         logger.warning('no "run_script" in "%s" section of config file, using default %s',config_section,self.runscript_filename)

      # read use_container
      if self.config.has_option(config_section,'use_container'):
         self.use_container = self.config.get(config_section,'use_container')
         logger.info('use_container: %s',self.use_container)
      else:
         logger.warning('no "use_container" in "%s" section of config file, using default %s',config_section,self.use_container)
      
      # read container_prefix
      if self.config.has_option(config_section,'container_prefix'):
         self.container_prefix = self.config.get(config_section,'container_prefix')
      elif self.use_container:
         raise Exception('must specify "container_prefix" in "%s" section of config file when "use_container" set to true' % config_section)
      logger.info('container_prefix: %s',self.container_prefix)

      # read run_elsewhere
      if self.config.has_option(config_section,'run_elsewhere'):
         self.run_elsewhere = self.config.get(config_section,'run_elsewhere')
         logger.info('run_elsewhere: %s',self.run_elsewhere)
      else:
         logger.warning('no "run_elsewhere" in "%s" section of config file, using default %s',config_section,self.run_elsewhere)

      # read run_directory
      if self.config.has_option(config_section,'run_directory'):
         self.run_directory = self.config.get(config_section,'run_directory')
      elif self.run_elsewhere:
         raise Exception('must specify "run_directory" in "%s" section of config file when "run_elsewhere" set to true' % config_section)
      logger.info('run_directory: %s',self.run_directory)

      # read logs_to_stage
      if self.config.has_option(config_section,'logs_to_stage'):
         self.logs_to_stage = self.config.get(config_section,'logs_to_stage')
         self.logs_to_stage = self.logs_to_stage.split(',')
         logger.info('logs_to_stage: %s',self.logs_to_stage)
      else:
         logger.warning('no "logs_to_stage" in "%s" section of config file, using default %s',config_section,self.run_elsewhere)

      # read droid subprocess_stdout:
      if self.config.has_option(config_section,'subprocess_stdout'):
         self.stdout_filename = self.config.get(config_section,'subprocess_stdout')
      else:
         raise Exception('must specify "subprocess_stdout" in "%s" section of config file' % config_section)
      logger.info('%s subprocess_stdout: %s',config_section,self.stdout_filename)

      # read droid subprocess_stderr:
      if self.config.has_option(config_section,'subprocess_stderr'):
         self.stderr_filename = self.config.get(config_section,'subprocess_stderr')
      else:
         raise Exception('must specify "subprocess_stderr" in "%s" section of config file' % config_section)
      logger.info('%s subprocess_stderr: %s',config_section,self.stderr_filename)

      # pipe the stdout/stderr from the Subprocess.Popen object to these files
      self.stdout_filename    = self.stdout_filename.format(rank=MPIService.rank,PandaID=self.job_def['PandaID'])
      # add working path to stdout filename if it is not already
      if not self.stdout_filename.startswith(self.droid_working_path):
         self.stdout_filename = os.path.join(self.droid_working_path,self.stdout_filename)
      logger.info('%s subprocess_stdout: %s',config_section,self.stdout_filename)

      # pipe Popen error to this file
      self.stderr_filename    = self.stderr_filename.format(rank=MPIService.rank,PandaID=self.job_def['PandaID'])
      # add working path to stderr filename if it is not already
      if not self.stderr_filename.startswith(self.droid_working_path):
         self.stderr_filename = os.path.join(self.droid_working_path,self.stderr_filename)
      logger.info('%s subprocess_stderr: %s',config_section,self.stderr_filename)

