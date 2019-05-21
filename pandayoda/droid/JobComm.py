# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)
# - Paul Nilsson (paul.nilsson@cern.ch)

import os
import os.path
import logging
import Queue
import shutil
import time
from pandayoda.common.yoda_multiprocessing import Event
from pandayoda.common import MessageTypes, EventRangeList, StatefulService, serializer

logger = logging.getLogger(__name__)


try:
    import yampl
except Exception:
    logger.exception("Failed to import yampl")
    raise

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


class NoEventRangeDefined(Exception):
    pass


class NoMoreEventsToProcess(Exception):
    pass


class FailedToParseYodaMessage(Exception):
    pass


class JobComm(StatefulService.StatefulService):
    """  JobComm: This thread handles all the AthenaMP related payloadcommunication

    Pilot/Droid launches AthenaMP and starts listening to its messages. AthenaMP finishes initialization and sends "Ready for events" to Pilot/Droid. Pilot/Droid sends back an event range. AthenaMP sends NWorkers-1 more times "Ready for events" to Pilot/Droid. On each of these messages Pilot/Droid replies with a new event range. After that all AthenaMP workers are busy processing events. Once some AthenaMP worker is available to take the next event range, AthenaMP sends "Ready for events" to Pilot/Droid. Pilot/Droid sends back an event range. Once some output file becomes available, AthenaMP sends full path, RangeID, CPU time and Wall time to Pilot/Droid and does not expect any answer on this message. Here is an example of such message:

    "/build1/tsulaia/20.3.7.5/run-es/athenaMP-workers-AtlasG4Tf-sim/worker_1/myHITS.pool.root_000.Range-6,ID:Range-6,CPU:1,WALL:1"

     If Pilot/Droid receives "Ready for events"and there are no more ranges to process, it answers with "No more events". This is a signal for AthenaMP that no more events are expected. In this case AthenaMP waits for all its workers to finish processing current event ranges, reports all outputs back to Pilot/Droid and exits.

     The event range format is json and is this: [{"eventRangeID": "8848710-3005316503-6391858827-3-10", "LFN":"EVNT.06402143._012906.pool.root.1", "lastEvent": 3, "startEvent": 3, "scope": "mc15_13TeV", "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"}]

    """

    WAITING_FOR_JOB = 'WAITING_FOR_JOB'
    REQUEST_EVENT_RANGES = 'REQUEST_EVENT_RANGES'
    WAITING_FOR_EVENT_RANGES = 'WAITING_FOR_EVENT_RANGES'
    MONITORING = 'MONITORING'
    WAIT_FOR_PAYLOAD_MESSAGE = 'WAIT_FOR_PAYLOAD_MESSAGE'
    MESSAGE_RECEIVED = 'MESSAGE_RECEIVED'
    SEND_EVENT_RANGE = 'SEND_EVENT_RANGE'
    SEND_OUTPUT_FILE = 'SEND_OUTPUT_FILE'
    EXITED = 'EXITED'

    STATES = [WAITING_FOR_JOB, WAITING_FOR_EVENT_RANGES,
              REQUEST_EVENT_RANGES, WAIT_FOR_PAYLOAD_MESSAGE,
              MESSAGE_RECEIVED, SEND_EVENT_RANGE, SEND_OUTPUT_FILE, EXITED]

    def __init__(self, config, queues, droid_working_path, droid_output_path, yampl_socket_name):
        """
          queues: A dictionary of SerialQueue.SerialQueue objects where the JobManager can send
                       messages to other Droid components about errors, etc.
          config: the ConfigParser handle for yoda
          droid_working_path: The location of the Droid working area
          droid_output_path: The location of output files from the Payload
        """
        # call base class init function
        super(JobComm, self).__init__()

        # dictionary of queues for sending messages to Droid components
        self.queues = queues

        # configuration of Yoda
        self.config = config

        # working path for droid
        self.working_path = droid_working_path

        # where output files will be placed if stage_outputs is set to True
        self.staging_path = droid_output_path

        # socket name to pass to transform for use when communicating via yampl
        self.yampl_socket_name = yampl_socket_name

        # flag to set when all work is done and thread is exiting
        self.all_work_done = Event()

        # set some defaults
        self.debug_message_char_length = 100
        self.stage_outputs = False

        # set initial state
        self.set_state(self.WAITING_FOR_JOB)

        # to be set
        self.loglevel = None

    def no_more_work(self):
        return self.all_work_done.is_set()

    def run(self):
        """ this is the function run as a subthread when the user runs jobComm_instance.start() """

        self.read_config()

        logger.debug('start yampl payloadcommunicator')
        athpayloadcomm = athena_payloadcommunicator(self.yampl_socket_name)
        payload_msg = ''

        # current list of output files to send via MPI
        output_files = []
        last_output_file_mpi_send = time.time()

        # list of event ranges
        eventranges = EventRangeList.EventRangeList()
        no_more_events = False
        waiting_for_eventranges = False
        event_range_request_counter = 0

        # current panda job that AthenaMP is configured to run
        current_job = None

        while not self.exit.is_set():
            logger.debug('start loop: state: %s',self.get_state())

            # in debug mode, report evenranges status
            if logger.getEffectiveLevel() == logging.DEBUG:
                ready_events = eventranges.number_ready()
                number_completed = eventranges.number_completed()
                total = len(eventranges)
                logger.debug('number of ready events %s; number of completed events %s; total events %s',ready_events,number_completed,total)

            # don't want to hammer Yoda with lots of little messages for output files
            # so aggregate output files for some time period then send as a group
            if len(output_files) == 0:
                last_output_file_mpi_send = time.time()
            elif (time.time() - last_output_file_mpi_send) > self.aggregate_output_files_time:

                # send output file data to Yoda/FileManager
                logger.info('sending %s output files to Yoda/FileManager',len(output_files))
                mpi_message = {'type':MessageTypes.OUTPUT_FILE,
                               'filelist':output_files,
                               'destination_rank': 0
                               }
                self.queues['MPIService'].put(mpi_message)

                # set time for next send
                last_output_file_mpi_send = time.time()
                # reset output file list
                output_files = []

            ##################
            # WAITING_FOR_JOB: waiting for the job definition to arrive, before
            #        it does, it is assumed that there is no payload running
            ######################################################################
            if self.get_state() == self.WAITING_FOR_JOB:
                logger.info(' waiting for job definition, blocking on message queue for %s ',self.loop_timeout)
                try:
                    qmsg = self.queues['JobComm'].get(block=True,timeout=self.loop_timeout)
                except Queue.Empty:
                    logger.debug('no message on queue')
                else:
                    # shorten our message for printing
                    if logger.getEffectiveLevel() == logging.DEBUG:
                        tmpmsg = str(qmsg)
                        if len(tmpmsg) > self.debug_message_char_length:
                            tmpslice = slice(0,self.debug_message_char_length)
                            tmpmsg = tmpmsg[tmpslice] + '...'
                        logger.debug('received queue message: %s',tmpmsg)

                    # verify message type is as expected
                    if 'type' not in qmsg or qmsg['type'] != MessageTypes.NEW_JOB or 'job' not in qmsg:
                        logger.error('received unexpected message format: %s',qmsg)
                    else:
                        logger.info('received job definition')
                        current_job = qmsg['job']

                        # change state
                        self.set_state(self.REQUEST_EVENT_RANGES)
                qmsg = None

            ##################
            # REQUEST_EVENT_RANGES: Request event ranges from Yoda
            ######################################################################
            elif self.get_state() == self.REQUEST_EVENT_RANGES:
                if not waiting_for_eventranges:
                    logger.info('sending request for event ranges')
                    # send MPI message to Yoda for more event ranges
                    self.request_events(current_job)
                    waiting_for_eventranges = True
                # change state
                self.set_state(self.WAITING_FOR_EVENT_RANGES)

            ##################
            # WAITING_FOR_EVENT_RANGES: Waiting for event ranges from Yoda
            ######################################################################
            elif self.get_state() == self.WAITING_FOR_EVENT_RANGES:
                logger.info('waiting for event ranges, blocking on message queue for %s',self.loop_timeout)
                try:
                    qmsg = self.queues['JobComm'].get(block=True,timeout=self.loop_timeout)
                except Queue.Empty:
                    logger.debug('no message on queue')
                else:
                    # shorten our message for printing
                    if logger.getEffectiveLevel() == logging.DEBUG:
                        tmpmsg = str(qmsg)
                        if len(tmpmsg) > self.debug_message_char_length:
                            tmpslice = slice(0,self.debug_message_char_length)
                            tmpmsg = tmpmsg[tmpslice] + '...'
                        logger.debug('received queue message: %s',tmpmsg)

                    if 'type' not in qmsg:
                        logger.error('received unexpected message format: %s',qmsg)
                    elif qmsg['type'] == MessageTypes.NEW_EVENT_RANGES:
                        logger.info('received event ranges, adding to list')
                        eventranges += EventRangeList.EventRangeList(qmsg['eventranges'])
                        # add event ranges to payload messenger list
                        # payloadcomm.add_eventranges(eventranges)
                        # change state
                        self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)
                    elif qmsg['type'] == MessageTypes.NO_MORE_EVENT_RANGES:
                        logger.info('no more event ranges for PandaID %s',qmsg['PandaID'])
                        no_more_events = True

                        # check for running events
                        if len(eventranges) == eventranges.number_completed():
                            logger.info('no eventranges left to send so triggering exit')
                            self.stop()
                        else:
                            logger.info('still have events to process so continuing')
                            self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)

                    else:
                        logger.error('unknown message type: %s',qmsg['type'])

                    waiting_for_eventranges = False


                qmsg = None

            ##################
            # WAIT_FOR_PAYLOAD_MESSAGE: initiates
            #          a request for a message from the payload
            ######################################################################
            if self.get_state() == self.WAIT_FOR_PAYLOAD_MESSAGE:

                # first check if there is an incoming message
                try:
                    logger.debug('checking for queue message')
                    qmsg = self.queues['JobComm'].get(block=False)
                    if MessageTypes.NEW_EVENT_RANGES in qmsg['type']:
                        logger.info('received new event range')
                        eventranges += EventRangeList.EventRangeList(qmsg['eventranges'])
                        waiting_for_eventranges = False
                    elif qmsg['type'] == MessageTypes.NO_MORE_EVENT_RANGES:
                        logger.info('no more event ranges for PandaID %s',qmsg['PandaID'])
                        no_more_events = True

                        # check for running events
                        if len(eventranges) == eventranges.number_completed():
                            logger.info('no eventranges left to send so triggering exit')
                            self.stop()
                        else:
                            logger.info('still have events to process so continuing')

                    else:
                        logger.error('received message of unknown type: %s',qmsg)
                except Queue.Empty:
                    logger.debug('no messages on queue')

                logger.info('checking for message from payload, block for %s, pending event range requests: %s',self.loop_timeout,event_range_request_counter)

                payload_msg = athpayloadcomm.recv(self.loop_timeout)

                if len(payload_msg) > 0:
                    logger.debug('received message: %s',payload_msg)
                    self.set_state(self.MESSAGE_RECEIVED)
                else:
                    logger.debug('did not receive message from payload')
                    if event_range_request_counter > 0:
                        logger.debug('have %s pending event range requests so will try sending one.',event_range_request_counter)
                        self.set_state(self.SEND_EVENT_RANGE)
                    # time.sleep(self.loop_timeout)

            ##################
            # MESSAGE_RECEIVED: this state indicates that a message has been
            #          received from the payload and its meaning will be parsed
            ######################################################################
            elif self.get_state() == self.MESSAGE_RECEIVED:

                # if ready for events, send them or wait for some
                if athena_payloadcommunicator.READY_FOR_EVENTS in payload_msg:
                    logger.info('payload is ready for event range')
                    self.set_state(self.SEND_EVENT_RANGE)
                    # increment counter to keep track of how many requests are queued
                    event_range_request_counter += 1

                #### OUTPUT File received
                elif len(payload_msg.split(',')) == 4:
                    # Athena sent details of an output file
                    logger.info('received output file from AthenaMP')
                    self.set_state(self.SEND_OUTPUT_FILE)

                else:
                    logger.error('failed to parse message from Athena: %s',payload_msg)
                    self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)

            ##################
            # SEND_EVENT_RANGE: wait until more event ranges are sent by JobComm
            ######################################################################
            elif self.get_state() == self.SEND_EVENT_RANGE:
                logger.debug('sending event to payload')
                # if event ranges available, send one
                try:
                    logger.debug('have %d ready event ranges to send to AthenaMP',eventranges.number_ready())
                    local_eventranges = eventranges.get_next()
                # no more event ranges available
                except EventRangeList.NoMoreEventRanges:
                    logger.debug('there are no more event ranges to process')
                    # if we have been told there are no more eventranges, then tell the AthenaMP worker there are no more events
                    if no_more_events:
                        logger.info('sending AthenaMP NO_MORE_EVENTS')
                        athpayloadcomm.send(athena_payloadcommunicator.NO_MORE_EVENTS)
                        # return to state requesting a message
                        self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)

                    # otherwise wait for more events
                    else:
                        logger.info('waiting for more events ranges')
                        self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)


                # something wrong with the index in the EventRangeList index
                except EventRangeList.RequestedMoreRangesThanAvailable:
                    logger.error('requested more event ranges than available, waiting for more event ranges')
                    self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)

                else:
                    logger.info('sending %s eventranges to AthenaMP',len(local_eventranges))
                    # append full path to file name for AthenaMP
                    # and adjust event counter by the number of files
                    # input_files = self.job_def.get()['inFiles'].split(',')
                    # logger.debug('%s: found %s input files',self.prelog,len(input_files))
                    for evtrg in local_eventranges:
                        evtrg['PFN'] = os.path.join(os.getcwd(),evtrg['LFN'])

                    # send AthenaMP the new event ranges
                    athpayloadcomm.send(serializer.serialize(local_eventranges))
                    # decrement counter since we sent some events
                    event_range_request_counter -= 1


                    # return to state requesting a message
                    self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)

                payload_msg = None

            ##################
            # SEND_OUTPUT_FILE: send output file data to MPIService
            ######################################################################
            elif self.get_state() == self.SEND_OUTPUT_FILE:
                logger.debug('send output file information')

                # parse message
                parts = payload_msg.split(',')
                # there should be four parts:
                # "myHITS.pool.root_000.Range-6,ID:Range-6,CPU:1,WALL:1"
                if len(parts) == 4:
                    # parse the parts
                    outputfilename = parts[0]
                    eventrangeid = parts[1].replace('ID:','')
                    cpu = parts[2].replace('CPU:','')
                    wallclock = parts[3].replace('WALL:','')

                    # if staging, stage and change output filename
                    if self.stage_outputs:
                        # move file to staging_path
                        logger.debug('shutil.move(%s,%s)',outputfilename,self.staging_path)
                        shutil.move(outputfilename,self.staging_path)
                        # change output filename
                        outputfilename = os.path.join(self.staging_path,os.path.basename(outputfilename))
                        logger.info('outputfilename - %s',outputfilename)

                    # build the data for Harvester output file
                    output_file_data = {'type':MessageTypes.OUTPUT_FILE,
                                        'filename':outputfilename,
                                        'eventrangeid':eventrangeid,
                                        'cpu':cpu,
                                        'wallclock':wallclock,
                                        'scope':current_job['scopeOut'],
                                        'pandaid':current_job['PandaID'],
                                        'eventstatus':'finished',
                                        'destination_rank': 0,
                                        }
                    # self.output_file_data.set(output_file_data)

                    # append output file data to list of files for transfer via MPI
                    output_files.append(output_file_data)
                    logger.info('received output file from AthenaMP; %s output files now on waiting list',len(output_files))

                    # set event range to completed:
                    logger.debug('mark event range id %s as completed',output_file_data['eventrangeid'])
                    try:
                        eventranges.mark_completed(output_file_data['eventrangeid'])
                    except Exception:
                        logger.error('failed to mark eventrangeid %s as completed', output_file_data['eventrangeid'])
                        self.stop()

                    # return to state requesting a message
                    self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)

                else:
                    logger.error('failed to parse output file')
                    self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)

                payload_msg = None

            # if ready_events is below the threshold and the no more events flag has not been set
            # request more event ranges
            if eventranges.number_ready() < self.get_more_events_threshold and not no_more_events and not waiting_for_eventranges and current_job is not None:
                logger.info('number of ready events %s below request threshold %s, asking for more.', eventranges.number_ready(), self.get_more_events_threshold)
                # send MPI message to Yoda for more event ranges
                self.request_events(current_job)
                waiting_for_eventranges = True

            # if the number of completed events equals the number of event ranges
            # available, and no more events flag is set, then kill subprocess and exit.
            elif eventranges.number_ready() == 0 and eventranges.number_completed() == len(eventranges) and no_more_events:
                logger.info('no more events to process, exiting')
                self.stop()
                self.all_work_done.set()
            # else:
            # logger.info('sleeping for %s',self.loop_timeout)
            # self.exit.wait(timeout=self.loop_timeout)

        # send any remaining output files to Yoda before exitingn.
        # don't want to hammer Yoda with lots of little messages for output files
        # so aggregate output files for some time period then send as a group
        if len(output_files) > 0:

            # send output file data to Yoda/FileManager
            logger.info('sending %s output files to Yoda/FileManager', len(output_files))
            mpi_message = {'type': MessageTypes.OUTPUT_FILE,
                           'filelist': output_files,
                           'destination_rank': 0
                           }
            self.queues['MPIService'].put(mpi_message)

            # reset output file list
            output_files = []

        self.set_state(self.EXITED)

        logger.info('JobComm exiting')

    def read_config(self):

        if config_section in self.config:
            # read loglevel:
            if 'loglevel' in self.config[config_section]:
                self.loglevel = self.config[config_section]['loglevel']
                logger.info('%s loglevel: %s',config_section, self.loglevel)
                logger.setLevel(logging.getLevelName(self.loglevel))
            else:
                logger.warning('no "loglevel" in "%s" section of config file, keeping default',config_section)

            # read loop_timeout:
            if 'loop_timeout' in self.config[config_section]:
                self.loop_timeout = int(self.config[config_section]['loop_timeout'])
                logger.info('%s loop_timeout: %s',config_section,self.loop_timeout)
            else:
                logger.warning('no "loop_timeout" in "%s" section of config file, keeping default %s',config_section,self.loop_timeout)

            # read get_more_events_threshold:
            if 'get_more_events_threshold' in self.config[config_section]:
                self.get_more_events_threshold = int(self.config[config_section]['get_more_events_threshold'])
                logger.info('%s get_more_events_threshold: %s',config_section,self.get_more_events_threshold)
            else:
                raise Exception('must specify "get_more_events_threshold" in "%s" section of config file' % config_section)

            # read aggregate_output_files_time:
            if 'aggregate_output_files_time' in self.config[config_section]:
                self.aggregate_output_files_time = int(self.config[config_section]['aggregate_output_files_time'])
                logger.info('%s aggregate_output_files_time: %s',config_section,self.aggregate_output_files_time)
            else:
                raise Exception('must specify "aggregate_output_files_time" in "%s" section of config file' % config_section)

            # read debug_message_char_length:
            if 'debug_message_char_length' in self.config[config_section]:
                self.debug_message_char_length = int(self.config[config_section]['debug_message_char_length'])
                logger.info('%s debug_message_char_length: %s',config_section,self.debug_message_char_length)
            else:
                logger.warning('no "debug_message_char_length" in "%s" section of config file, using default %s',config_section,self.debug_message_char_length)

            # read stage_outputs:
            if 'stage_outputs' in self.config[config_section]:
                self.stage_outputs = self.get_boolean(self.config[config_section]['stage_outputs'])
                logger.info('%s stage_outputs: %s',config_section,self.stage_outputs)
            else:
                logger.warning('no "stage_outputs" in "%s" section of config file, using default %s',config_section,self.stage_outputs)

        else:
            raise Exception('no %s section in the configuration' % config_section)

    def get_boolean(self, string):
        if 'true' in string.lower():
            return True
        return False

    def request_events(self, current_job):
        msg = {
            'type': MessageTypes.REQUEST_EVENT_RANGES,
            'PandaID': current_job['PandaID'],
            'taskID': current_job['taskID'],
            'jobsetID': current_job['jobsetID'],
            'destination_rank': 0,  # YODA rank
        }
        self.queues['MPIService'].put(msg)

    def send_output_file(self, payload_msg,current_job, eventranges,output_files):
        logger.debug('sending output file information')

        # parse message
        parts = payload_msg.split(',')
        # there should be four parts:
        # "myHITS.pool.root_000.Range-6,ID:Range-6,CPU:1,WALL:1"
        if len(parts) == 4:
            # parse the parts
            outputfilename = parts[0]
            eventrangeid = parts[1].replace('ID:', '')
            cpu = parts[2].replace('CPU:', '')
            wallclock = parts[3].replace('WALL:', '')

            # if staging, stage and change output filename
            if self.stage_outputs:
                # move file to staging_path
                logger.debug('shutil.move(%s,%s)', outputfilename, self.staging_path)
                shutil.move(outputfilename, self.staging_path)
                # change output filename
                outputfilename = os.path.join(self.staging_path, os.path.basename(outputfilename))
                logger.info('outputfilename - %s', outputfilename)

            # build the data for Harvester output file
            output_file_data = {'type': MessageTypes.OUTPUT_FILE,
                                'filename': outputfilename,
                                'eventrangeid': eventrangeid,
                                'cpu': cpu,
                                'wallclock': wallclock,
                                'scope': current_job['scopeOut'],
                                'pandaid': current_job['PandaID'],
                                'eventstatus': 'finished',
                                'destination_rank': 0,
                                }
            # self.output_file_data.set(output_file_data)

            # append output file data to list of files for transfer via MPI
            output_files.append(output_file_data)
            logger.info('received output file from AthenaMP; %s output files now on waiting list', len(output_files))

            # return to state requesting a message
            # self.set_state(self.WAIT_FOR_PAYLOAD_MESSAGE)

            # set event range to completed:
            logger.debug('mark event range id %s as completed',output_file_data['eventrangeid'])
            try:
                eventranges.mark_completed(output_file_data['eventrangeid'])
            except Exception:
                logger.error('failed to mark eventrangeid %s as completed', output_file_data['eventrangeid'])
                self.stop()

        else:
            logger.error('failed to parse output file')

    def send_eventrange(self, eventranges, athpayloadcomm, no_more_events):
        logger.debug('sending event to payload')
        # if event ranges available, send one
        try:
            logger.debug('have %d ready event ranges to send to AthenaMP', eventranges.number_ready())
            local_eventranges = eventranges.get_next()

        # no more event ranges available
        except EventRangeList.NoMoreEventRanges:
            logger.debug('there are no more event ranges to process')
            # if we have been told there are no more eventranges, then tell the AthenaMP worker there are no more events
            if no_more_events:
                logger.info('sending AthenaMP NO_MORE_EVENTS')
                athpayloadcomm.send(athena_payloadcommunicator.NO_MORE_EVENTS)
            else:
                # otherwise, raise the Exception to trigger an event request
                raise

        # something wrong with the index in the EventRangeList index
        except EventRangeList.RequestedMoreRangesThanAvailable:
            logger.error('requested more event ranges than available, going to try waiting for more events')
            raise EventRangeList.NoMoreEventRanges()

        else:
            logger.info('sending eventranges to AthenaMP: %s',local_eventranges)
            # append full path to file name for AthenaMP
            # and adjust event counter by the number of files
            # input_files = self.job_def.get()['inFiles'].split(',')
            # logger.debug('found %s input files',len(input_files))
            for evtrg in local_eventranges:
                evtrg['PFN'] = os.path.join(os.getcwd(),evtrg['LFN'])

            # send AthenaMP the new event ranges
            athpayloadcomm.send(serializer.serialize(local_eventranges))


class athena_payloadcommunicator:
    """ small class to handle yampl payloadcommunication exception handling """
    READY_FOR_EVENTS = 'Ready for events'
    NO_MORE_EVENTS = 'No more events'
    FAILED_PARSE = 'ERR_ATHENAMP_PARSE'

    def __init__(self, socketname='EventService_EventRanges', context='local'):

        # create server socket for yampl
        try:
            self.socket = yampl.ServerSocket(socketname, context)
        except Exception:
            logger.exception('failed to create yampl server socket')
            raise

    def send(self, message):
        # send message using yampl
        try:
            self.socket.send_raw(message)
        except Exception:
            logger.exception("Failed to send yampl message: %s",message)
            raise

    def recv(self, timeout=0):
        # receive yampl message, timeout is in milliseconds
        t1 = time.time()
        buf = ''
        while time.time() < (t1 + timeout):
            size, buf = self.socket.try_recv_raw()
            if size == -1:
                time.sleep(1)
            else:
                break
        return str(buf)
