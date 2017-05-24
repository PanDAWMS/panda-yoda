
import EventRange

class NoMoreEventRanges(Exception): pass
class RequestedMoreRangesThanAvailable(Exception): pass

class EventRangeList(object):
   def __init__(self,eventranges = None):
      ''' initialize object, can pass optional argument:
            eventranges:      list that looks like this
                 [{"eventRangeID": "8848710-3005316503-6391858827-3-10", "LFN":"EVNT.06402143._012906.pool.root.1", "lastEvent": 3, "startEvent": 3, "scope": "mc15_13TeV", "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"}]

          some warnings: this class assumes that the list of EventRange objects is always orded by the 
                        EventRange.STATES list, with READY being at the end.
      '''

      # internal list of EventRange objects
      self.eventranges = []
      # keep track of the next ready index
      self.next_ready = 0

      # internal list of indices of those ranges which have already been assigned to droid ranks
      # indices point back to self.eventranges
      self.indices_of_assigned_ranges = []
      self.indices_of_completed_ranges = []
      if eventranges:
         self.fill_from_list(eventranges)

   def number_ready(self):
      ''' provide the number of ranges ready to be assigned '''
      return len(self.eventranges) - len(self.indices_of_assigned_ranges) - len(self.indices_of_completed_ranges)

   def number_processing(self):
      ''' provide the number of ranges still to be completed '''
      return len(self.eventranges) - len(self.indices_of_completed_ranges)

   def fill_from_list(self,list_of_eventrange_dicts):
      for eventrange in list_of_eventrange_dicts:
         self.append(EventRange.EventRange(eventrange))

   def mark_completed(self,eventRangeID):
      for eventrange in self.eventranges:
         if eventrange.id == eventRangeID:
            eventrange.state = EventRange.EventRange.COMPLETED
            break

   def get_next(self,number_of_ranges=1):
      ''' method for retrieving number_of_ranges worth of event ranges,
          which will be marked as 'assigned'
      '''
      if self.next_ready >= len(self.eventranges):
         raise NoMoreEventRanges(' next_ready = %d; len(eventranges) = %d' % (self.next_ready,len(self.eventranges)))
      if self.next_ready + number_of_ranges >= len(self.eventranges):
         raise RequestedMoreRangesThanAvailable
      output = []
      for i  in range(self.next_ready,self.next_ready+number_of_ranges):
         eventRange = self.eventranges[i]
         eventRange.state = EventRange.EventRange.ASSIGNED
         self.indices_of_assigned_ranges.append(i)
         output.append(eventRange.get_dict())
      self.next_ready += number_of_ranges

      return output

   def __add__(self,other):
      if isinstance(other,EventRangeList):
         newone = EventRangeList()
         
         # combine range lists, ording by state
         # get all ranges in ready state
         ranges_by_state = {}
         for state in EventRange.EventRange.STATES:
            # get all EventRange objects from self in this state
            ranges_by_state[state] = [x for x in self.eventranges if x.state == state]
            # add all EventRange objects from other in this state
            ranges_by_state[state] += [x for x in other.eventranges if x.state == state]

         # use order of EventRange.EventRange.STATES to organize the new list
         for state in EventRange.EventRange.STATES:
            # if this is the READY state, set the new index
            if state == EventRange.EventRange.READY:
               newone.ready_index = len(newone.eventranges)
            elif state == EventRange.EventRange.ASSIGNED:
               newone.indices_of_assigned_ranges += range(len(newone.eventranges),len(newone.eventranges)+len(ranges_by_state[state]))
            elif state == EventRange.EventRange.COMPLETED:
               newone.indices_of_completed_ranges += range(len(newone.eventranges),len(newone.eventranges)+len(ranges_by_state[state]))
            newone.eventranges += ranges_by_state[state]

         return newone
      else:
         raise TypeError('other is not of type EventRangeList: %s' % type(eventRange).__name__)


   def append(self,eventRange):
      if isinstance(eventRange,EventRange.EventRange):
         self.eventranges.append(eventRange)
      else:
         raise TypeError('object is not of type EventRange: %s' % type(eventRange).__name__)


   def pop(self,key=None):
      return self.eventranges.pop(key)
   def remove(self,value):
      self.remove(value)
   def __iter__(self,key):
      return iter(self.eventranges)
   def __len__(self):
      return len(self.eventranges)
   def __getitem(self,key):
      return self.eventranges[key]
   def __setitem__(self,key,value):
      if isinstance(value,EventRange.EventRange):
         self.eventranges[key] = value
      else:
         raise TypeError('object is not of type EventRange: %s' % type(eventRange).__name__)
   def __delitem__(self,key):
      del self.eventranges[key]


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
        {"eventRangeID": "8848710-3005316503-6391858827-3-10", 
         "LFN":"EVNT.06402143._012906.pool.root.1", 
         "lastEvent": 4, 
         "startEvent": 4, 
         "scope": "mc15_13TeV", 
         "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"},
        {"eventRangeID": "8848710-3005316503-6391858827-3-10", 
         "LFN":"EVNT.06402143._012906.pool.root.1", 
         "lastEvent": 5, 
         "startEvent": 5, 
         "scope": "mc15_13TeV", 
         "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"},
        {"eventRangeID": "8848710-3005316503-6391858827-3-10", 
         "LFN":"EVNT.06402143._012906.pool.root.1", 
         "lastEvent": 6, 
         "startEvent": 6, 
         "scope": "mc15_13TeV", 
         "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"},
        {"eventRangeID": "8848710-3005316503-6391858827-3-10", 
         "LFN":"EVNT.06402143._012906.pool.root.1", 
         "lastEvent": 7, 
         "startEvent": 7, 
         "scope": "mc15_13TeV", 
         "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"},
       ]


   erl.fill_from_list(l)

   logger.info('n-ready: %d  n-processing: %d',erl.number_ready(),erl.number_processing())

   er = erl.get_next()
   logger.info('er = %s',er)

   logger.info('n-ready: %d  n-processing: %d',erl.number_ready(),erl.number_processing())

   er = erl.get_next(2)

   logger.info('n-ready: %d  n-processing: %d',erl.number_ready(),erl.number_processing())

   logger.info(' testing add function' )
   erl2 = EventRangeList()
   erl2.fill_from_list(l)
   er = erl2.get_next(2)
   logger.info('2 n-ready: %d  n-processing: %d',erl2.number_ready(),erl2.number_processing())

   erl3 = erl + erl2
   logger.info('3 n-ready: %d  n-processing: %d',erl3.number_ready(),erl3.number_processing())





