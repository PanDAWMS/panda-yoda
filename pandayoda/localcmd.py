import logging,os,sys
logger = logging.getLogger(__name__)


# locally defined command to run for the job
def getLocalCmd(job):

   cmd = ''
   cmd += getLocalEnv(job)
   cmd += ';' + getTrfSetup(job)
   cmd += ';' + getTrfCmd(job)
   
def getTrfSetup(job):
   swrelease=job['swRelease'].split('-')[1]
   cmd = ''
   swbase = os.environ.get('HPC_SW_INSTALL_AREA')
   if swbase is None:
      raise Exception('no software install area found, define envornment variable: HPC_SW_INSTALL_AREA')
   if not os.path.exists(swbase):
      raise Exception('software area does not exist: HPC_SW_INSTALL_AREA = %s'%swbase)
   
   if   swrelease.startswith('21'):
      appdir = "%s/%s/AtlasSetup" %(swbase,'.'.join(x for x in swrelease.split('.')[0:2]))
   elif swrelease.startswith('19'):
      appdir = '%s/%s/%s/AtlasSetup' % (swbase,'x86_64-slc6-gcc47-opt',swrelease)

   asetup_cmd = 'source ' + appdir + '/scripts/asetup.sh'
   post_asetup_cmd = ''

   asetup_options = ''
   if 'homepackage' in job:
      x = job['homepackage'].split('/')
      package = x[0]
      full_release = x[1]

      asetup_options += ' ' + package + ',' + full_release

   if full_release.startswith('19'):
      asetup_options += ' --cmtextratags=ATLAS,useDBRelease'
   elif full_release.startswith('21'):
      swreleasebase = os.path.join(swbase,swrelease.split('.')[0:2])
      asetup_options += ' --releasesarea=%s' % swreleasebase
      asetup_options += ' --cmakearea=%s/sw/lcg/contrib/CMake' % swreleasebase
      asetup_options += ' --gcclocation=%s/sw/lcg/releases/gcc/4.9.3/x86_64-slc6' % swreleasebase
      post_asetup_cmd += '; DBBASEPATH=/global/cscratch1/sd/parton/atlas/repo/sw/database/DBRelease/current'
      #asetup_options += '; export CORAL_DBLOOKUP_PATH=$DBBASEPATH/XMLConfig'
      #asetup_options += '; export CORAL_AUTH_PATH=$DBBASEPATH/XMLConfig'
      post_asetup_cmd += '; export DATAPATH=$DBBASEPATH:$DATAPATH'
      post_asetup_cmd += ';export FRONTIER_SERVER="(serverurl=http://atlasfrontier-ai.cern.ch:8000/atlr)(serverurl=http://lcgft-atlas.gridpp.rl.ac.uk:3128/frontierATLAS)(proxyurl=http://pc1806.nersc.gov:3128)"'

   cmd = asetup_cmd + asetup_options + post_asetup_cmd
   return cmd

# build TRF Command
def getTrfCmd(job):
   trf_cmd = job['transformation']
   # for release 21 we need to remove '--DBRelease="default:current"'
   if job['swRelease'].split('-')[1].startswith('21'):
      for par in job['jobPars']:
         if 'DBRelease' in par and 'current' in par:
            continue
         trf_cmd += ' ' + par
   else:
      trf_cmd += ' ' + job['jobPars']

   return trf_cmd

# pre-athena environment variables
def getLocalEnv(job):

   env_cmd = ''

   env_cmd += ';export PANDA_RESOURCE=NERSC_Edison_2'
   if 'taskID' in job and 'jobID' in job:
      env_cmd += ';export FRONTIER_ID="[%s_%s]"' % (job['jobID'],job['taskID'])
      env_cmd += ';export CMSSW_VERSION=$FRONTIER_ID'
   env_cmd += ';export CMSSW_VERSION=$FRONTIER_ID'
   env_cmd += ';export CMSSW_VERSION=$FRONTIER_ID'
   if 'processingType' in job:
      env_cmd += ';export RUCIO_APPID=%s' % job['processingType']
   env_cmd += ';export RUCIO_ACCOUNT=' + os.environ.get('RUCIO_ACCOUNT','pilot')

   return env_cmd


