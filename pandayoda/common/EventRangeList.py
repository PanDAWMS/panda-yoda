import logging
logger = logging.getLogger(__name__)
import EventRange

class NoMoreEventRanges(Exception): pass
class RequestedMoreRangesThanAvailable(Exception): pass
class EventRangeIdNotFound(Exception): pass

class EventRangeList(object):
   def __init__(self,eventranges = None):
      ''' initialize object, can pass optional argument:
            eventranges:      list that looks like this
                 [{"eventRangeID": "8848710-3005316503-6391858827-3-10", "LFN":"EVNT.06402143._012906.pool.root.1", "lastEvent": 3, "startEvent": 3, "scope": "mc15_13TeV", "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"}]

          some warnings: this class assumes that the list of EventRange objects is always orded by the 
                        EventRange.STATES list, with READY being at the end.
      '''

      # internal list of EventRange objects key-ed by id
      self.eventranges = {}
      
      # internal list of indices of those ranges which have already been assigned to droid ranks
      # indices point back to self.eventranges
      self.ids_by_state = {}
      for state in EventRange.EventRange.STATES:
         self.ids_by_state[state] = []
      
      if eventranges:
         self.fill_from_list(eventranges)


   def number_processing(self):
      ''' provide the number of ranges still to be completed '''
      return len(self.eventranges) - self.number_completed()

   def number_assigned(self):
      ''' provide the number of ranges assigned '''
      return len(self.ids_by_state[EventRange.EventRange.ASSIGNED])

   def number_completed(self):
      ''' provide the number of ranges completed '''
      return len(self.ids_by_state[EventRange.EventRange.COMPLETED])

   def number_ready(self):
      ''' provide the number of ranges completed '''
      return len(self.ids_by_state[EventRange.EventRange.READY])


   def fill_from_list(self,list_of_eventrange_dicts):
      for eventrange in list_of_eventrange_dicts:
         self.append(EventRange.EventRange(eventrange))

   def change_eventrange_state(self,eventRangeID,new_state):
      if eventRangeID in self.eventranges:
         logger.debug('changing id %s to %s',eventRangeID,new_state)
         eventrange = self.eventranges[eventRangeID]
         logger.debug('current state of id %s is %s',eventRangeID,eventrange.state)
         self.ids_by_state[eventrange.state].remove(eventrange.id)
         eventrange.state = new_state
         self.ids_by_state[eventrange.state].append(eventrange.id)
      else:
         raise EventRangeIdNotFound('eventRangeID %s not found' % eventRangeID)

   def mark_completed(self,eventRangeID):
      logger.debug('marking eventRangeID %s as completed',eventRangeID)
      self.change_eventrange_state(eventRangeID,EventRange.EventRange.COMPLETED)

   def mark_assigned(self,eventRangeID):
      logger.debug('marking eventRangeID %s as assigned',eventRangeID)
      self.change_eventrange_state(eventRangeID,EventRange.EventRange.ASSIGNED)

   def get_next(self,number_of_ranges=1):
      ''' method for retrieving number_of_ranges worth of event ranges,
          which will be marked as 'assigned'
      '''
      logger.debug('getting %d event ranges.',number_of_ranges)
      if self.number_ready() <= 0:
         raise NoMoreEventRanges(' number ready = %d; len(eventranges) = %d' % (self.number_ready(),len(self.eventranges)))
      if self.number_ready() < number_of_ranges:
         raise RequestedMoreRangesThanAvailable
      output = []
      for i  in range(number_of_ranges):
         # pop one id off the ready list
         id = self.ids_by_state[EventRange.EventRange.READY].pop()

         # add the dictionary from the event range for that id to the output
         output.append(self.eventranges[id].get_dict())
         self.eventranges[id].set_assigned()
         self.ids_by_state[EventRange.EventRange.ASSIGNED].append(id)

         logger.debug('marked id %s as assigned',id)
      
      return output

   def __add__(self,other):
      if isinstance(other,EventRangeList):
         newone = EventRangeList()

         # combine range lists
         for eventrangeid,eventrange in self.iteritems():
            newone.append(eventrange)
         for eventrangeid,eventrange in other.iteritems():
            newone.append(eventrange)
         
         # combine the lists of ids_by_state
         for state in EventRange.EventRange.STATES:
            newone.ids_by_state[state] = self.ids_by_state[state] + other.ids_by_state[state]

         return newone
      else:
         raise TypeError('other is not of type EventRangeList: %s' % type(eventRange).__name__)


   def append(self,eventRange):
      if isinstance(eventRange,EventRange.EventRange):
         self.eventranges[eventRange.id] = eventRange
         self.ids_by_state[eventRange.state].append(eventRange.id)
      else:
         raise TypeError('object is not of type EventRange: %s' % type(eventRange).__name__)


   def pop(self,key,default=None):
      return self.eventranges.pop(key,default)
   def iteritems(self):
      return self.eventranges.iteritems()
   def keys(self):
      return self.eventranges.keys()
   def values(self):
      return self.eventranges.values()
   def get(self,key,default=None):
      return self.eventranges.get(key,default)
   def has_key(self,key):
      return self.eventranges.has_key(key)

   def __iter__(self,key):
      return iter(self.eventranges)
   def __len__(self):
      return len(self.eventranges)
   def __getitem__(self,key):
      return self.eventranges[key]
   def __setitem__(self,key,value):
      if isinstance(value,EventRange.EventRange):
         self.eventranges[key] = value
      else:
         raise TypeError('object is not of type EventRange: %s' % type(value).__name__)
   def __delitem__(self,key):
      del self.eventranges[key]
   def __contains__(self,key):
      return self.eventranges.__contains__(key)


# testing this thread
if __name__ == '__main__':
   import logging,time
   logging.basicConfig(level=logging.DEBUG,
         format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
         datefmt='%Y-%m-%d %H:%M:%S')
   logger = logging.getLogger(__name__)
   logger.info('Start test of EventRangeList')

   #import argparse
   #oparser = argparse.ArgumentParser()
   #oparser.add_argument('-l','--jobWorkingDir', dest="jobWorkingDir", default=None, help="Job's working directory.",required=True)
   #args = oparser.parse_args()

   logger.info('test object creation')
   erl = EventRangeList()

   logger.info('test fill_from_list ')
   l = [
        {"eventRangeID": "8848710-3005316503-6391858827-3-10", 
         "LFN":"EVNT.06402143._012906.pool.root.1", 
         "lastEvent": 3, 
         "startEvent": 3, 
         "scope": "mc15_13TeV", 
         "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"},
        {"eventRangeID": "8848710-3005316503-6391858827-4-10", 
         "LFN":"EVNT.06402143._012906.pool.root.1", 
         "lastEvent": 4, 
         "startEvent": 4, 
         "scope": "mc15_13TeV", 
         "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"},
        {"eventRangeID": "8848710-3005316503-6391858827-5-10", 
         "LFN":"EVNT.06402143._012906.pool.root.1", 
         "lastEvent": 5, 
         "startEvent": 5, 
         "scope": "mc15_13TeV", 
         "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"},
        {"eventRangeID": "8848710-3005316503-6391858827-6-10", 
         "LFN":"EVNT.06402143._012906.pool.root.1", 
         "lastEvent": 6, 
         "startEvent": 6, 
         "scope": "mc15_13TeV", 
         "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"},
        {"eventRangeID": "8848710-3005316503-6391858827-7-10", 
         "LFN":"EVNT.06402143._012906.pool.root.1", 
         "lastEvent": 7, 
         "startEvent": 7, 
         "scope": "mc15_13TeV", 
         "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"},
       ]


   erl.fill_from_list(l)

   logger.info('n-ready: %d  n-processing: %d',erl.number_ready(),erl.number_processing())

   ers = erl.get_next()
   logger.info('ers = %s',ers)

   logger.info('n-ready: %d  n-processing: %d',erl.number_ready(),erl.number_processing())
   erl.mark_completed(ers[0]['eventRangeID'])

   logger.info('n-ready: %d  n-processing: %d',erl.number_ready(),erl.number_processing())

   ers = erl.get_next(2)
   logger.info('ers = %s',ers)

   logger.info('n-ready: %d  n-processing: %d',erl.number_ready(),erl.number_processing())

   for er in ers:
      erl.mark_completed(er['eventRangeID'])

   logger.info('n-ready: %d  n-processing: %d',erl.number_ready(),erl.number_processing())

   logger.info(' testing add function' )
   erl2 = EventRangeList()
   erl2.fill_from_list(l)
   er = erl2.get_next(2)
   logger.info('2 n-ready: %d  n-processing: %d',erl2.number_ready(),erl2.number_processing())

   erl3 = erl + erl2
   logger.info('3 n-ready: %d  n-processing: %d',erl3.number_ready(),erl3.number_processing())





