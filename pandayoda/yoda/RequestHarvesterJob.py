import logging, os, time
from pandayoda.common import StatefulService, exceptions, MessageTypes
from pandayoda.common import yoda_multiprocessing as mp

logger = logging.getLogger(__name__)

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


class RequestHarvesterJob(StatefulService.StatefulService):
    ''' This thread is spawned to request jobs from Harvester '''

    CREATED = 'CREATED'
    REQUEST = 'REQUEST'
    WAITING = 'WAITING'
    GETTING_JOB = 'GETTING_JOB'
    EXITED = 'EXITED'

    STATES = [CREATED, REQUEST, WAITING, GETTING_JOB, EXITED]
    RUNNING_STATES = [REQUEST, WAITING, GETTING_JOB]

    def __init__(self, config, queues, mpmgr, harvester_messenger):
        super(RequestHarvesterJob, self).__init__()

        # local config options
        self.config = config

        # dictionary of queues for sending messages to Droid components
        self.queues = queues

        # messenger module for communicating with harvester
        self.harvester_messenger = harvester_messenger

        # set current state of the thread to CREATED
        self.set_state(self.CREATED)

        # if in state REQUEST_COMPLETE, this variable holds the job retrieved
        self.mgr = mpmgr
        self.new_jobs = self.mgr.dict()
        self.jobs_avail = mp.Event()

        # set if there are no more jobs coming from Harvester
        self.no_more_jobs_flag = mp.Event()

    def get_jobs(self):
        ''' parent thread calls this function to retrieve the jobs sent by Harevester '''
        return self.new_jobs

    def set_jobs(self, job_descriptions):
        for key in job_descriptions.keys():
            self.new_jobs[key] = job_descriptions[key]
            self.jobs_avail.set()

    def jobs_ready(self):
        return self.jobs_avail.is_set()

    def exited(self):
        return self.in_state(self.EXITED)

    def running(self):
        if self.get_state() in self.RUNNING_STATES:
            return True
        return False

    def no_more_jobs(self):
        return self.no_more_jobs_flag.is_set()

    def run(self):
        ''' overriding base class function '''

        if config_section in self.config:
            # read log level:
            if 'loglevel' in self.config[config_section]:
                self.loglevel = self.config[config_section]['loglevel']
                logger.info('%s loglevel: %s', config_section, self.loglevel)
                logger.setLevel(logging.getLevelName(self.loglevel))
            else:
                logger.warning('no "loglevel" in "%s" section of config file, keeping default', config_section)

            # read droid loop timeout:
            if 'loop_timeout' in self.config[config_section]:
                self.loop_timeout = int(self.config[config_section]['loop_timeout'])
                logger.info('%s loop_timeout: %s', config_section, self.loop_timeout)
            else:
                logger.warning('no "loop_timeout" in "%s" section of config file, keeping default %s', config_section,
                               self.loop_timeout)

        else:
            raise Exception('no %s section in the configuration' % config_section)

        # start in the request state
        self.set_state(self.REQUEST)

        while not self.exit.is_set():
            # get state
            logger.debug('start loop, current state: %s', self.get_state())

            #########
            # REQUEST State
            ########################
            if self.get_state() == self.REQUEST:
                logger.info('making request for job')

                # first check that there is not a job ready
                if not self.harvester_messenger.pandajobs_ready():
                    try:
                        # use messenger to request jobs from Harvester
                        self.harvester_messenger.request_jobs()
                    except exceptions.MessengerJobAlreadyRequested:
                        logger.warning('job already requested.')

                    # wait for events
                    self.set_state(self.WAITING)
                else:
                    # since panda job is already ready, retrieve job
                    self.set_state(self.GETTING_JOB)

            #########
            # WAITING State
            ########################
            elif self.get_state() == self.WAITING:
                logger.debug('checking if request is complete')
                # use messenger to check if jobs are ready
                if self.harvester_messenger.pandajobs_ready():
                    logger.debug('jobs are ready')
                    # since panda job is already ready, retrieve job
                    self.set_state(self.GETTING_JOB)
                else:
                    logger.info('no response yet, sleep for %s', self.loop_timeout)
                    time.sleep(self.loop_timeout)


            #########
            # GETTING_JOB State
            ########################
            elif self.get_state() == self.GETTING_JOB:
                logger.info('getting job')

                # use messenger to get jobs from Harvester
                pandajobs = self.harvester_messenger.get_pandajobs()

                # set jobs for parent and change state
                if len(pandajobs) > 0:
                    logger.debug('setting NEW_JOBS variable')
                    self.set_jobs(pandajobs)
                    logger.debug('sending WorkManager WAKE_UP message')
                    self.queues['WorkManager'].put({'type': MessageTypes.WAKE_UP})
                    logger.debug('triggering exit')
                    self.stop()
                else:
                    logger.debug('no jobs returned: %s', pandajobs)
                    self.stop()

            else:
                logger.info('nothing to do, sleeping for %s', self.loop_timeout)
                self.exit.wait(timeout=self.loop_timeout)

        self.set_state(self.EXITED)
        logger.debug('RequestHarvesterJob thread is exiting')
