from multiprocessing import Queue
from Queue import Empty
import serializer
# from Queue import Queue,Full,Empty


class SerialQueue(Queue):
   pass


class SerialQueue2(Queue,object):
   ''' custom version of Queue class which takes python objects and serializes them into strings '''
   def put(self,message,block=True,timeout=None):
      smsg = serializer.serialize(message)
      super(SerialQueue,self).put(smsg,block=block,timeout=timeout)

   def get(self,block=True,timeout=None):
      smsg = super(SerialQueue,self).get(block=block,timeout=timeout)
      return serializer.deserialize(smsg)
