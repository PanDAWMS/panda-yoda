import os,logging,subprocess
from pandayoda.common import VariableWithLock,MPIService,StatefulService
logger = logging.getLogger(__name__)


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
                     loop_timeout,
                     stdout_filename,
                     stderr_filename,
                     yampl_socket_name):
      '''
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send
                     messages to other Droid components about errors, etc.
        config: the ConfigParser handle for yoda
        droid_working_path: the path where athena will execute
        '''
      # call base class init function
      super(TransformManager,self).__init__(loop_timeout)

      # job definition from panda
      self.job_def               = job_def

      # extract runscript template from config
      self.runscript_template    = config.get('TransformManager','template')

      # extract runscript filename
      self.runscript_filename    = config.get('TransformManager','run_script')

      # extract run_with_container
      self.use_container         = config.getboolean('TransformManager','use_container')

      # extract container prefix command
      self.container_prefix      = config.get('TransformManager','container_prefix')

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # working path for athenaMP
      self.droid_working_path    = droid_working_path

      # working path for Yoda (where input files are located)
      self.yoda_working_path     = yoda_working_path

      # loop timeout for monitoring etc
      self.loop_timeout          = loop_timeout

      # pipe the stdout/stderr from the Subprocess.Popen object to these files
      self.stdout_filename    = stdout_filename.format(rank=MPIService.rank,PandaID=job_def['PandaID'])
      # add working path to stdout filename if it is not already
      if not self.stdout_filename.startswith(self.droid_working_path):
         self.stdout_filename = os.path.join(self.droid_working_path,self.stdout_filename)
      
      # pipe Popen error to this file
      self.stderr_filename    = stderr_filename.format(rank=MPIService.rank,PandaID=job_def['PandaID'])
      # add working path to stderr filename if it is not already
      if not self.stderr_filename.startswith(self.droid_working_path):
         self.stderr_filename = os.path.join(self.droid_working_path,self.stderr_filename)


      # socket name to pass to transform for use when communicating via yampl
      self.yampl_socket_name     = yampl_socket_name

      # return code, set only after exit
      self.returncode            = VariableWithLock.VariableWithLock()

      # set state to initial
      self.set_state(TransformManager.CREATED)

   def get_returncode(self):
      self.returncode.get()

   def run(self):
      ''' start and monitor transform in subprocess '''
      self.set_state(TransformManager.STARTED)

      # first step, create subprocess
      logger.info('create and start subprocess for transform')
      self.start_subprocess()

      # third step, loop to monitor process
      logger.info('monitoring subprocess for transform')
      self.set_state(TransformManager.MONITORING)
      while not self.exit.wait(timeout=self.loop_timeout):
         logger.debug('start moinitor loop')

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

      jobPars = self.job_def['jobPars'][:start_index] + ','.join(x for x in updated_input_files) + self.job_def['jobPars'][end_index:]

      # insert the Yampl AthenaMP setting
      if "--preExec" not in jobPars:
         jobPars += " --preExec \'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\"\' " % self.yampl_socket_name
      else:
         if "import jobproperties as jps" in jobPars:
            jobPars = jobPars.replace("import jobproperties as jps;", "import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\";" % self.yampl_socket_name)
         else:
            jobPars = jobPars.replace("--preExec ", "--preExec \'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\"\' " % self.yampl_socket_name)
      
      # these prints are too long with jumbo jobs
      # logger.debug('jobPars: %s',self.job_def['jobPars'])
      # logger.debug('new jobPars: %s',jobPars)


      
      script = template.format(transformation=transformation,
                               jobPars=jobPars,
                               cmtConfig=self.job_def['cmtConfig'],
                               release=release,
                               package=package,
                               processingType=self.job_def['processingType'],
                               pandaID=self.job_def['PandaID'],
                               taskID=self.job_def['taskID'],
                               gcclocation=gcclocation
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
      cmd = self.create_job_run_script()

      # if container job add container prefix command
      if self.use_container:
         cmd = self.container_prefix + ' ' + cmd

      logger.debug('starting run_script: %s',cmd)
      
      self.jobproc = subprocess.Popen(['/bin/bash',cmd],
                     stdout=open(self.stdout_filename,'w'),
                     stderr=open(self.stderr_filename,'w'),
                     cwd=self.droid_working_path)

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




   
