from mpi4py import MPI
import os,logging,array
logger = logging.getLogger(__name__)
''' 
This module should provide all the messaging functions for communication between Yoda & Droid.
The current implementation uses MPI, but could easily be replaced with another form.
'''

YODA_RANK = 0

FROM_DROID = 1
FROM_YODA = 2

REQUEST_JOB_MESSAGE = 'READY_FOR_JOB'
NO_WORK_LEFT = 'NO_WORK_LEFT'

NEW_JOB_TYPE = 'NEW_JOB'


def send_job_request():
   msg = {'type':REQUEST_JOB_MESSAGE}
   return send_message(msg,dest=YODA_RANK,tag=FROM_DROID)

def send_droid_new_job(job,droid_rank):
   msg = {'type':NEW_JOB_TYPE,'job':job}
   return send_message(msg,dest=droid_rank,tag=FROM_YODA)

def send_droid_no_work_left(droid_rank):
   msg = {'type':NO_WORK_LEFT}
   return send_message(msg,dest=droid_rank,tag=FROM_YODA)

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
      return MPI.COMM_WORLD.irecv(source=source,tag=tag)
   except:
      logger.exception('Rank %05i: exception received while trying to receive a message.',MPI.COMM_WORLD.Get_rank())
      raise

