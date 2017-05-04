import json,logging
from mpi4py import MPI
logger = logging.getLogger(__name__)

def serialize(msg):
   try:
      return json.dumps(msg)
   except:
      logger.exception('Rank %d: failed to serialize the message: %s',MPI.COMM_WORLD.Get_rank(),msg)
      raise


def deserialize(msg):
   try:
      return json.loads(msg)
   except:
      logger.exception('Rank %d: failed to deserialize the message: %s',MPI.COMM_WORLD.Get_rank(),msg)
      raise

