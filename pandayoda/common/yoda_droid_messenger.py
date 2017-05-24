from mpi4py import MPI
import os,logging,array
from pandayoda.common import MessageTypes
logger = logging.getLogger(__name__)
''' 
This module should provide all the messaging functions for communication between Yoda & Droid.
The current implementation uses MPI, but could easily be replaced with another form.
'''

# Yoda is always Rank 0
YODA_RANK            = 0

# Message tags
FROM_DROID           = 1
FROM_YODA            = 2

TO_YODA_WORKMANAGER  = 3
FROM_YODA_WORKMANAGER= 4



# droid sends job request to yoda
def send_job_request():
   msg = {'type':MessageTypes.REQUEST_JOB}
   return send_message(msg,dest=YODA_RANK,tag=TO_YODA_WORKMANAGER)

# yoda receives job request from droid
def recv_job_request():
   return receive_message(MPI.ANY_SOURCE,tag=TO_YODA_WORKMANAGER)

# droid sends event ranges request to yoda
def send_eventranges_request():
   msg = {'type':MessageTypes.REQUEST_EVENT_RANGES}
   return send_message(msg,dest=YODA_RANK,tag=TO_YODA_WORKMANAGER)

# yoda receives event ranges request from droid
def recv_eventranges_request():
   return receive_message(MPI.ANY_SOURCE,tag=TO_YODA_WORKMANAGER)

# yoda sends new job to droid
def send_droid_new_job(job,droid_rank):
   msg = {'type':MessageTypes.NEW_JOB,'job':job}
   return send_message(msg,dest=droid_rank,tag=FROM_YODA_WORKMANAGER)

# droid receieves new job from yoda
def recv_job():
   return receive_message(YODA_RANK,FROM_YODA_WORKMANAGER)

# yoda sends new event ranges to droid
def send_droid_new_eventranges(eventranges,droid_rank):
   msg = {'type':MessageTypes.NEW_EVENT_RANGES,'eventranges':eventranges}
   return send_message(msg,dest=droid_rank,tag=FROM_YODA_WORKMANAGER)

# droid receives new event ranges from yoda
def recv_eventranges():
   return receive_message(YODA_RANK,FROM_YODA_WORKMANAGER)

def send_droid_no_job_left(droid_rank):
   msg = {'type':MessageTypes.NO_MORE_JOBS}
   return send_message(msg,dest=droid_rank,tag=FROM_YODA_WORKMANAGER)

def send_droid_no_eventranges_left(droid_rank):
   msg = {'type':MessageTypes.NO_MORE_EVENT_RANGES}
   return send_message(msg,dest=droid_rank,tag=FROM_YODA_WORKMANAGER)

def get_droid_message():
   return receive_message(MPI.ANY_SOURCE,FROM_DROID)





def send_message(data,dest=None,tag=None):
   ''' basic MPI_ISend but mpi4py handles the object tranlation for sending 
       over MPI so your message can be python objects.
         data: this is the object you want to send, e.g. a dictionary, list, class object, etc.
         dest: this is the destination rank
         tag:  this tag can be used to filter messages
      return: returns a Request object which is used to test for communication completion,
              through a blocking call, Request.wait(), and and a non-blocking call, Request.test(). '''
   try:
      return MPI.COMM_WORLD.isend(data,dest=dest,tag=tag)
   except:
      logger.exception('Rank %05i: exception received during sending request for a job.',MPI.COMM_WORLD.Get_rank())
      raise

def receive_message(source=MPI.ANY_SOURCE,tag=None):
   ''' basic MPI_ISend but mpi4py handles the object tranlation for sending 
       over MPI so your message can be python objects.
         source: this is the source rank
         tag:  this tag can be used to filter messages
      return: returns a Request object which is used to test for communication completion,
              through a blocking call, Request.wait(), and and a non-blocking call, Request.test(). '''
   # using MPI_Recv
   try:
      if tag is not None:
         return MPI.COMM_WORLD.irecv(source=source,tag=tag)
      else:
         return MPI.COMM_WORLD.irecv(source=source)
   except:
      logger.exception('Rank %05i: exception received while trying to receive a message.',MPI.COMM_WORLD.Get_rank())
      raise

