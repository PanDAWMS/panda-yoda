# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)
# - Paul Nilsson (paul.nilsson@cern.ch)

import os
import logging
import Queue
from pandayoda.common.yoda_multiprocessing import Process, Event, Manager
import RequestHarvesterJob
import RequestHarvesterEventRanges
import PandaJobDict
from pandayoda.common import MessageTypes, EventRangeList
logger = logging.getLogger(__name__)

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


class WorkManager(Process):
    """ Work Manager: this thread manages work going to the running Droids """

    def __init__(self, config, queues, harvester_messenger):
        """
        queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send
                       messages to other Droid components about errors, etc.
        config: the ConfigParser handle for yoda
        """

        # call base class init function
        super(WorkManager, self).__init__()

        # dictionary of queues for sending messages to Droid components
        self.queues = queues

        # configuration of Yoda
        self.config = config

        # harvester communication module
        self.harvester_messenger = harvester_messenger

        # this is used to trigger the thread exit
        self.exit = Event()

        # to be set
        self.pending_index = None
        self.pending_requests = None

    def stop(self):
        """ This function can be called by outside subthreads to cause the JobManager thread to exit """
        self.exit.set()

    def run(self):  # noqa: C901
        """ This function is executed as the subthread. """

        # read inputs from config file
        self.read_config()

        # list of all jobs received from Harvester key-ed by panda id
        pandajobs = PandaJobDict.PandaJobDict()

        # pending requests from droid ranks
        self.pending_requests = []
        # helps track which request I am processing
        self.pending_index = 0

        # place holder for Request Harevester Event Ranges instance (wait for job definition before launching)
        requestharvestereventranges = None

        # create a local multiprocessing manager for shared values
        mpmgr = Manager()

        # start a Request Havester Job thread to begin getting a job
        requestharvesterjob = RequestHarvesterJob.RequestHarvesterJob(self.config, self.queues, mpmgr, self.harvester_messenger)
        requestharvesterjob.start()

        while not self.exit.is_set():
            logger.debug('start loop')
            ################
            # check for queue messages
            ################################
            qmsg = None
            if not self.queues['WorkManager'].empty():
                logger.info('queue has messages')
                qmsg = self.queues['WorkManager'].get(block=False)
                # any time I get a message from this queue, I reset the index of the pending request list
                # this way, I cycle through the pending requests once per new message
                self.pending_index = 0
            # if main queue is empty, process pending message queues
            elif len(self.pending_requests) > 0:
                if self.pending_index < len(self.pending_requests):
                    logger.debug('pending queue has %s messages processing %s', len(self.pending_requests), self.pending_index)
                    qmsg = self.pending_requests[self.pending_index]
                else:
                    logger.info('have cycled through all pending requests without a change, will block on queue for %s', self.loop_timeout)
                    try:
                        self.pending_index = 0
                        qmsg = self.queues['WorkManager'].get(block=True, timeout=self.loop_timeout)
                    except Queue.Empty:
                        logger.debug('no messages on queue after blocking')
            else:
                # if not (
                #  (requestharvesterjob is not None and requestharvesterjob.jobs_ready()) and
                #  (requestharvestereventranges is not None and requestharvestereventranges.eventranges_ready())
                #      ):
                try:
                    self.pending_index = 0
                    logger.info('blocking on queue for %s', self.loop_timeout)
                    qmsg = self.queues['WorkManager'].get(block=True, timeout=self.loop_timeout)
                except Queue.Empty:
                    logger.debug('no messages on queue after blocking')

            if qmsg:
                logger.debug('received message %s', qmsg)

                #############
                ## Just a message to cause WorkManager to wake from sleep and process
                ###############################
                if qmsg['type'] == MessageTypes.WAKE_UP:
                    continue

                #############
                ## DROID requesting new job
                ###############################
                elif qmsg['type'] == MessageTypes.REQUEST_JOB:
                    logger.debug('droid requesting job description')

                    # Do I have a panda job to give out?
                    # if not, create a new request if no request is active
                    if len(pandajobs) == 0:
                        # if there are no panda jobs and requestHarvesterJob is None, then start a request
                        # this really should never happen.
                        logger.debug('There are no panda jobs')
                        if requestharvesterjob is None:
                            logger.info('launching new job request')
                            requestharvesterjob = RequestHarvesterJob.RequestHarvesterJob(self.config, self.queues, mpmgr, self.harvester_messenger)
                            requestharvesterjob.start()
                        else:
                            if requestharvesterjob.running():
                                logger.debug('request is running, adding message to pending and will process again later')
                            elif requestharvesterjob.exited():
                                logger.debug('request has exited')
                                jobs = requestharvesterjob.get_jobs()
                                if jobs is None:
                                    logger.error('request has exited and returned no events, reseting request object')
                                    if requestharvesterjob.is_alive():
                                        requestharvesterjob.stop()
                                        logger.info('waiting for requestHarvesterJob to join')
                                        requestharvesterjob.join()
                                    requestharvesterjob = None
                                else:
                                    logger.info('new jobs ready, adding to PandaJobDict, then add to pending requests')
                                    pandajobs.append_from_dict(jobs)
                                    # add to pending requests because the job to send will be chose by another
                                    # section of code below which sends the job based on the event ranges on hand

                                    # reset job request
                                    requestharvesterjob = None
                                    # backup pending counter
                                    self.pending_index += -1

                            else:
                                if requestharvesterjob.state_lifetime() > 60:
                                    logger.error('request is stuck in state %s recreating it.', requestharvesterjob.get_state())
                                    if requestharvesterjob.is_alive():
                                        requestharvesterjob.stop()
                                        logger.info('waiting for requestHarvesterJob to join')
                                        requestharvesterjob.join()
                                        requestharvesterjob = None
                                else:
                                    logger.debug('request is in %s state, waiting', requestharvesterjob.get_state())

                        logger.debug('pending message')
                        # place request on pending_requests queue and reprocess again when job is ready
                        self.pend_request(qmsg)

                    # There are jobs in the list so choose one to send
                    # The choice depends on numbers of events available for each job
                    elif len(pandajobs) == 1:
                        logger.debug('There is one job so send it.')

                        # get the job
                        # FUTUREDEV: It's unclear if we will ever run more than one PandaID per Yoda job
                        # so in the future this may need to actually search the pandajobs list for the
                        # one with the most jobs to send or something like that.
                        pandaid = pandajobs.keys()[0]
                        job = pandajobs[pandaid]

                        # send it to droid rank
                        logger.info('sending droid rank %s panda id %s which has the most ready events %s',
                                    qmsg['source_rank'], pandaid, job.number_ready())
                        outmsg = {
                            'type': MessageTypes.NEW_JOB,
                            'job': job.job_def,
                            'destination_rank': qmsg['source_rank']
                        }
                        self.queues['MPIService'].put(outmsg)

                        if qmsg in self.pending_requests:
                            self.pending_requests.remove(qmsg)
                        qmsg = None

                    # There are jobs in the list so choose one to send
                    # The choice depends on numbers of events available for each job
                    elif len(pandajobs) > 1:
                        logger.error('there are multiple jobs to choose from, this is not yet implemented')
                        raise Exception('there are multiple jobs to choose from, this is not yet implemented')

                #############
                ## DROID requesting new event ranges
                ###############################
                elif qmsg['type'] == MessageTypes.REQUEST_EVENT_RANGES:
                    logger.debug('droid requesting event ranges')

                    # the droid sent the current running panda id, determine if there are events left for this panda job
                    droid_pandaid = str(qmsg['PandaID'])

                    # if there are no event ranges left reply with such
                    if pandajobs[droid_pandaid].eventranges.no_more_event_ranges:
                        logger.debug('no event ranges left for panda ID %s', droid_pandaid)
                        logger.info('sending NO_MORE_EVENT_RANGES to rank %s', qmsg['source_rank'])
                        self.queues['MPIService'].put(
                            {'type': MessageTypes.NO_MORE_EVENT_RANGES,
                             'destination_rank': qmsg['source_rank'],
                             'PandaID': droid_pandaid,
                             })

                        # remove message if from pending
                        if qmsg in self.pending_requests:
                            self.pending_requests.remove(qmsg)
                        qmsg = None

                    # may still be event ranges for this ID
                    else:
                        logger.debug('retrieving event ranges for panda ID %s', droid_pandaid)

                        # event ranges found for pandaID
                        if str(droid_pandaid) in pandajobs.keys():
                            logger.debug('EventRangeList object for pandaID %s exists, events ready %s', droid_pandaid, pandajobs[droid_pandaid].number_ready())

                            # have event ranges ready
                            if pandajobs[droid_pandaid].number_ready() > 0:
                                self.send_eventranges(pandajobs[droid_pandaid].eventranges, qmsg)

                                # remove message if from pending
                                if qmsg in self.pending_requests:
                                    self.pending_requests.remove(qmsg)
                                qmsg = None

                            # no event ranges remaining, will request more
                            else:
                                logger.debug('no eventranges remain for pandaID %s, can we request more?', droid_pandaid)

                                # check if the no more event ranges flag has been set for this panda id
                                if pandajobs[droid_pandaid].eventranges.no_more_event_ranges:
                                    # set the flag so we know there are no more events for this PandaID
                                    logger.debug('no more event ranges for PandaID: %s', droid_pandaid)
                                    logger.debug('sending NO_MORE_EVENT_RANGES to rank %s', qmsg['source_rank'])
                                    self.queues['MPIService'].put(
                                        {'type': MessageTypes.NO_MORE_EVENT_RANGES,
                                         'destination_rank': qmsg['source_rank'],
                                         'PandaID': droid_pandaid,
                                         })

                                    # remove message if from pending
                                    if qmsg in self.pending_requests:
                                        self.pending_requests.remove(qmsg)
                                    qmsg = None

                                # flag is not set, so if no request is running create one
                                elif requestharvestereventranges is None:
                                    logger.debug('requestHarvesterEventRanges does not exist, creating new request')
                                    requestharvestereventranges = RequestHarvesterEventRanges.RequestHarvesterEventRanges(
                                        self.config,
                                        {
                                            'pandaID': pandajobs[droid_pandaid]['PandaID'],
                                            'jobsetID': pandajobs[droid_pandaid]['jobsetID'],
                                            'taskID': pandajobs[droid_pandaid]['taskID'],
                                            'nRanges': self.request_n_eventranges,
                                        },
                                        mpmgr,
                                        self.harvester_messenger,
                                    )
                                    requestharvestereventranges.start()

                                    # pend the request for later processing
                                    self.pend_request(qmsg)
                                # there is a request, if it is running, pend the message for later processing
                                elif requestharvestereventranges.running():
                                    logger.debug('requestHarvesterEventRanges is running, will pend this request and check again')
                                    self.pend_request(qmsg)
                                # there is a request, if it is has exited, check if there are events available
                                elif requestharvestereventranges.exited():
                                    logger.debug('requestHarvesterEventRanges exited, will check for new event ranges')

                                    requestharvestereventranges.join()

                                    # if no more events flag is set, there are no more events for this PandaID
                                    if requestharvestereventranges.no_more_eventranges():
                                        # set the flag so we know there are no more events for this PandaID
                                        logger.debug('no more event ranges for PandaID: %s', droid_pandaid)
                                        pandajobs[droid_pandaid].eventranges.no_more_event_ranges = True
                                        logger.debug('sending NO_MORE_EVENT_RANGES to rank %s', qmsg['source_rank'])
                                        self.queues['MPIService'].put(
                                            {'type': MessageTypes.NO_MORE_EVENT_RANGES,
                                             'destination_rank': qmsg['source_rank'],
                                             'PandaID': droid_pandaid,
                                             })

                                        # remove message if from pending
                                        if qmsg in self.pending_requests:
                                            self.pending_requests.remove(qmsg)
                                        qmsg = None

                                    # event ranges received so add them to the list
                                    else:
                                        tmpeventranges = requestharvestereventranges.get_eventranges()

                                        if tmpeventranges is not None:
                                            logger.debug('received eventranges: %s',
                                                         ' '.join(('%s:%i' % (tmpid, len(tmpeventranges[tmpid])))
                                                                  for tmpid in tmpeventranges.keys()))
                                            # add event ranges to pandajobs dict
                                            for jobid in tmpeventranges.keys():
                                                ers = tmpeventranges[jobid]
                                                pandajobs[jobid].eventranges += EventRangeList.EventRangeList(ers)
                                                # events will be sent in the next loop execution

                                            self.send_eventranges(pandajobs[jobid].eventranges, qmsg)

                                            # remove message if from pending
                                            if qmsg in self.pending_requests:
                                                self.pending_requests.remove(qmsg)
                                            qmsg = None
                                        else:
                                            logger.error('no eventranges after requestHarvesterEventRanges exited, starting new request')
                                            self.pend_request(qmsg)

                                    # reset request
                                    requestharvestereventranges = None

                                else:
                                    logger.error('requestHarvesterEventRanges is in strange state %s, restarting', requestharvestereventranges.get_state())
                                    requestharvestereventranges = None
                                    self.pend_request(qmsg)

                        # something went wrong
                        else:
                            logger.error('there is no eventrange for pandaID %s, this should be impossible since every pandaID in the '
                                         'pandajobs dictionary gets an empty EventRangeList object. '
                                         'Something is amiss. panda job ids: %s', droid_pandaid, pandajobs.keys())

                else:
                    logger.error('message type was not recognized: %s', qmsg['type'])

            # if there is nothing to be done, sleep

            # if (requestharvesterjob is not None and requestharvesterjob.running()) and \
            #   (requestharvestereventranges is not None and requestharvestereventranges.running()) and \
            #   self.queues['WorkManager'].empty() and self.pending_requests.empty():
            #   time.sleep(self.loop_timeout)
            # else:
            logger.debug('continuing loop')
            if requestharvesterjob is not None:
                logger.debug('RequestHarvesterJob: %s', requestharvesterjob.get_state())
            if requestharvestereventranges is not None:
                logger.debug('requestHarvesterEventRanges: %s', requestharvestereventranges.get_state())

        logger.info('signaling exit to threads')
        if requestharvesterjob is not None and requestharvesterjob.is_alive():
            logger.debug('signaling requestHarvesterJob to stop')
            requestharvesterjob.stop()
        if requestharvestereventranges is not None and requestharvestereventranges.is_alive():
            logger.debug('signaling requestHarvesterEventRanges to stop')
            requestharvestereventranges.stop()

        if requestharvesterjob is not None and requestharvesterjob.is_alive():
            logger.debug('waiting for requestHarvesterJob to join')
            requestharvesterjob.join()
        if requestharvestereventranges is not None and requestharvestereventranges.is_alive():
            logger.debug('waiting for requestHarvesterEventRanges to join')
            requestharvestereventranges.join()

        logger.info('WorkManager is exiting')

    def number_eventranges_ready(self, eventranges):
        total = 0
        for id, range in eventranges.iteritems():
            total += range.number_ready()
        return total

    def get_jobid_with_minimum_ready(self, eventranges):

        # loop over event ranges, count the number of ready events
        job_id = 0
        job_nready = 999999
        for pandaid, erl in eventranges.iteritems():
            nready = erl.number_ready()
            if nready > 0 and nready < job_nready:
                job_id = pandaid
                job_nready = nready

        return job_id

    def send_eventranges(self, eventranges, qmsg):
        """ Send the requesting MPI rank some event ranges. """
        # get a  subset of ready eventranges up to the send_n_eventranges value
        local_eventranges = eventranges.get_next(min(eventranges.number_ready(), self.send_n_eventranges))

        # send event ranges to Droid
        logger.info('sending %d new event ranges to droid rank %d', len(local_eventranges), qmsg['source_rank'])
        outmsg = {
            'type': MessageTypes.NEW_EVENT_RANGES,
            'eventranges': local_eventranges,
            'destination_rank': qmsg['source_rank'],
        }
        self.queues['MPIService'].put(outmsg)

    def read_config(self):

        if config_section in self.config:
            # read log level:
            if 'loglevel' in self.config[config_section]:
                self.loglevel = self.config[config_section]['loglevel']
                logger.info('%s loglevel: %s', config_section, self.loglevel)
                logger.setLevel(logging.getLevelName(self.loglevel))
            else:
                logger.warning('no "loglevel" in "%s" section of config file, keeping default', config_section)

            # read loop timeout:
            if 'loop_timeout' in self.config[config_section]:
                self.loop_timeout = int(self.config[config_section]['loop_timeout'])
                logger.info('%s loop_timeout: %s', config_section, self.loop_timeout)
            else:
                logger.warning('no "loop_timeout" in "%s" section of config file, keeping default %s', config_section, self.loop_timeout)

            # read send_n_eventranges:
            if 'send_n_eventranges' in self.config[config_section]:
                self.send_n_eventranges = int(self.config[config_section]['send_n_eventranges'])
                logger.info('%s send_n_eventranges: %s', config_section, self.send_n_eventranges)
            else:
                raise Exception('configuration section %s has no "send_n_eventranges" setting. '
                                'This setting is important and should be optimized to your system. '
                                'Typically you should set it to the number of AthenaMP workers on a single node or some factor of that.' % config_section)

            # read request_n_eventranges:
            if 'request_n_eventranges' in self.config[config_section]:
                self.request_n_eventranges = int(self.config[config_section]['request_n_eventranges'])
                logger.info('%s request_n_eventranges: %s', config_section, self.request_n_eventranges)
            else:
                raise Exception('configuration section %s has no "request_n_eventranges" setting. '
                                'This setting is important and should be optimized to your system. '
                                'Typically you should set it to the number of AthenaMP workers on a single node multiplied by the '
                                'total number of Droid ranks running or some factor of that.' % config_section)

        else:
            raise Exception('no %s section in the configuration' % config_section)

    def pend_request(self, msg):
        if msg in self.pending_requests:
            self.pending_index += 1
        else:
            self.pending_requests.append(msg)
            self.pending_index = 0
