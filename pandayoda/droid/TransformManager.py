import os,logging,subprocess
logger = logging.getLogger(__name__)
from pandayoda.common import VariableWithLock,MPIService,StatefulService

class TransformManager(StatefulService.StatefulService):
   ''' This service is handed a job definition and when the parent process
       calls the start() function, it launches the transform defined by the
       panda job.'''

   CREATED     = 'CREATED'
   STARTED     = 'STARTED'
   MONITORING  = 'MONITORING'
   FINISHED    = 'FINISHED'
   STATES=[CREATED,STARTED,MONITORING,FINISHED]

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

      # dictionary of queues for sending messages to Droid components
      self.queues                = queues

      # working path for athenaMP
      self.droid_working_path    = droid_working_path

      # working path for Yoda (where input files are located)
      self.yoda_working_path     = yoda_working_path

      # loop timeout for monitoring etc
      self.loop_timeout          = loop_timeout

      # pipe the stdout/stderr from the Subprocess.Popen object to these files
      self.stdout_filename       = stdout_filename
      self.stderr_filename       = stderr_filename

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
      logger.info('creating subprocess for transform')
      jobproc = JobSubProcess(self.job_def,
                              self.droid_working_path,
                              self.yoda_working_path,
                              self.stdout_filename,
                              self.stderr_filename,
                              self.yampl_socket_name,
                              self.runscript_template,
                              self.runscript_filename
                             )

      # second step, launch subprocess
      logger.info('starting subprocess for transform')
      jobproc.start()

      # third step, loop to monitor process
      logger.info('monitoring subprocess for transform')
      self.set_state(TransformManager.MONITORING)
      while not self.exit.wait(timeout=self.loop_timeout):
         logger.debug('start moinitor loop')

         if not jobproc.is_running():
            logger.info('job has finished')
            break
      
      self.set_state(TransformManager.FINISHED)

      # incase we are exiting because parent process told us to exit
      # we should kill the subprocess
      if self.exit.isSet() and jobproc.is_running():
         logger.info('signaled to exit before subprocess exited so killing it.')
         jobproc.kill()

      self.returncode.set(jobproc.returncode)

      logger.info('exiting')



class JobSubProcess:
   ''' Runs a subprocess.Popen process based on the job definition passed
       and pipes the output to the filenames for stdout and stderr '''
   def __init__(self,job_def,
                  droid_working_path,
                  yoda_working_path,
                  stdout_filename,
                  stderr_filename,
                  yampl_socket_name,
                  runscript_template,
                  runscript_filename):
      
      # panda job definition
      self.job_def            = job_def
      
      # working path for this process
      self.droid_working_path = droid_working_path

      # working path for yoda (input file location)
      self.yoda_working_path  = yoda_working_path
      
      # pipe Popen output to this file
      self.stdout_filename    = stdout_filename.format(rank=MPIService.rank,PandaID=job_def['PandaID'])
      # add working path to stdout filename if it is not already
      if not self.stdout_filename.startswith(self.droid_working_path):
         self.stdout_filename = os.path.join(self.droid_working_path,self.stdout_filename)
      
      # pipe Popen error to this file
      self.stderr_filename    = stderr_filename.format(rank=MPIService.rank,PandaID=job_def['PandaID'])
      # add working path to stderr filename if it is not already
      if not self.stderr_filename.startswith(self.droid_working_path):
         self.stderr_filename = os.path.join(self.droid_working_path,self.stderr_filename)

      # yampl socket name
      self.yampl_socket_name  = yampl_socket_name
      
      # the script template for launching the subprocess
      self.runscript_template = runscript_template

      # the script filename to use to store the customized version of the template
      self.runscript_filename = runscript_filename

      # place holder for job process object
      self.jobproc            = None

   def kill(self):
      if self.jobproc:
         self.jobproc.kill()
      else:
         raise Exception('tried killing subprocess, but subprocess is empty')

   def is_running(self):
      if self.jobproc is None: 
         return False
      # check if job is still running
      if self.jobproc.poll() is None: 
         return True
      return False

   def returncode(self):
      if self.jobproc is None:
         raise Exception('tried to get return code, but subprocess is empty')
      return self.jobproc.returncode

   def start(self):
      ''' start the job subprocess, handling the command parsing and log file naming '''
      

      # parse the job into a command
      cmd = self.create_job_run_script()
      logger.debug('starting run_script: %s',cmd)
      
      self.jobproc = subprocess.Popen(['/bin/bash',cmd],
                     stdout=open(self.stdout_filename,'w'),
                     stderr=open(self.stderr_filename,'w'),
                     cwd=self.droid_working_path)


   def create_job_run_script(self):
      ''' using the template set in the configuration, create a script that 
          will run the panda job with the appropriate environment '''
      template_filename = self.runscript_template
      if os.path.exists(template_filename):
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
         #elif release.startswith('20'):

         # set transformation command
         transformation = self.job_def['transformation']

         # replace input file with full path
         input_files = self.job_def['inFiles'].split(',')
         updated_input_files = []
         for input_file in input_files:
            updated_input_files.append(os.path.join(self.yoda_working_path,input_file))

         logger.debug('inFiles: %s',input_files)
         logger.debug('new files: %s',updated_input_files)

         start_index = self.job_def['jobPars'].find('--inputEVNTFile=') + len('--inputEVNTFile=')
         end_index   = self.job_def['jobPars'].find(' ',start_index)

         jobPars = self.job_def['jobPars'][:start_index] + ','.join(x for x in updated_input_files) + self.job_def['jobPars'][end_index:]

         # insert the Yampl AthenaMP setting
         if not "--preExec" in jobPars:
            jobPars += " --preExec \'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\"\' " % self.yampl_socket_name
         else:
            if "import jobproperties as jps" in jobPars:
               jobPars = jobPars.replace("import jobproperties as jps;", "import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\";" % self.yampl_socket_name)
            else:
               jobPars = jobPars.replace("--preExec ", "--preExec \'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\"\' " % self.yampl_socket_name)
         
         logger.debug('jobPars: %s',self.job_def['jobPars'])
         logger.debug('new jobPars: %s',jobPars)


         
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

         return script_filename
      else:
         raise Exception('specified template does not exist: %s',template_filename)
