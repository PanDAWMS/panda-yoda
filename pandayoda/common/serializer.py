import json,logging
logger = logging.getLogger(__name__)

def serialize(msg,pretty_print=False):
   try:
      if pretty_print:
         return json.dumps(msg,indent=2,sort_keys=True)
      else:
         return json.dumps(msg)
   except:
      logger.exception('failed to serialize the message: %s',msg)
      raise


def deserialize(msg):
   try:
      return json.loads(msg)
   except:
      logger.exception('failed to deserialize the message: %s',msg)
      raise

