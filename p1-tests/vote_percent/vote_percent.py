from pyspark import SparkContext
from functools import partial
from collections import defaultdict

import StringIO
import csv
import datetime
import permute.interval as interval
import pandas

votes_file = '2012-curr-full-votes.csv'
#master = "local[4]" 
master = "spark://ec2-54-83-184-241.compute-1.amazonaws.com:7077"
def load_votes(context):
    votes_data = context.textFile(votes_file, use_unicode=False).cache()
    return votes_data

def keyByBillId(line):
    """
    Key by the bill id to be used in the self join step. The tuple looks like:
    (bill_id, (person_id, vote, date))
    """
    df = '%Y-%m-%d'    
    input = StringIO.StringIO(line)
    reader = csv.reader(input)
    tup = reader.next()
    if tup:
      tup[6] = datetime.datetime.strptime(tup[6], df).date()
    reduced = []
    #person id
    reduced.append(tup[1])
    #date
    reduced.append(tup[6])
    #vote (yay, nay, etc)
    reduced.append(tup[2])
    return (tup[0], reduced)

def keyByPerson(line):
    df = '%Y-%m-%d'    
    input = StringIO.StringIO(line)
    reader = csv.reader(input)
    tup = reader.next()
    if tup:
      tup[6] = datetime.datetime.strptime(tup[6], df).date()
    l = tup[2:]
    val = tup[0]
    l.insert(0, val)
    return (tup[1], l)

def rekeyByBillId((person_id, bill_info)):
    val = person_id
    l = bill_info[1:]
    l.insert(0, val)
    return (bill_info[0], l)

def count_votes((key, joined_tuple)):
    """ 
    Map function that counts the votes between two entries.
    Called on a self-joined bills entry, so the incoming tuple looks like:
    (bill_id, ((person_id, vote, date), (person_id, vote, date))
    """
    left = joined_tuple[0]
    right = joined_tuple[1]
    left_id = left[0]
    right_id = right[0]
    key = left_id + ":" + right_id
    agree = 0
    disagree = 0
    if left[2] == right[2]:
        agree = 1
    else:
        disagree = 1
    return (key, (agree, disagree, left[1]))

def reduce_count(left, right):
    agree = left[0] + right[0]
    dis = left[1] + right[1]
    pct = 0
    if agree + dis >= 0:
      pct = agree / float((agree + dis))
    return (agree, dis, pct)

def filter_count((key, (agree, dis, pct))):
  """ 
  Filter any entries that don't have at least 50 votes. 
  This is also needed because some entries get filtered out completely
  and so the previous reduce_count stage is not called for some tuples, 
  resulting in the third entry in this tuple being a date object instead
  of a percent. 
  """
  if agree + dis < 50:
    return False
  return True

def filter_join((key, (left, right))):
  """
  Avoids duplicate calculations/entries
  ex: ("p_id_1:p_id_2", "p_id_2:p_id_1"), removes the right one.
  """
  if left[0] < right[0]:
     return True
  return False

def filter_by_int((key, (agree, disagree, vote_date)), start=None, end=None):
  if vote_date >= start and vote_date <= end:
    return True
  return False

def run(context):
    """ Data is in the following format: (bill_id, person_id, vote, type, chamber, year, date, session, status, extra).
        1. Key everything by bill_id, parse the date correctly, return (bill_id, (person_id, vote, date))
        2. Self join bills RDD to itself.
        3. Remove duplicate entries (via left.person_id < right.person_id) and comparisons with self.
        4. Map the joined data to a (person_id:person_id, (agree, disagree)) RDD.
        5. Iterate through the intervals, calculating (agree/(agree + disagree)) for each key in 4. 
        6. Store a dictionary, for each key, store the lowest current percent and interval it was found in.
    """
    raw_votes = load_votes(context)
    intervals = interval.interval_set('1/1/2012', '1/1/2014', freq='15D', max_delta=pandas.Timedelta(days=120))
    bills = raw_votes.map(keyByBillId)
    joined = bills.join(bills, 32)
    joined = joined.filter(filter_join)
    counted = joined.map(count_votes)

    pct_map = defaultdict(float)
    interval_map = dict()
    """
    Now we iterate through the intervals one by one. A better choice might
    be to join a bunch of intervals at once (ie do the calculations in batches)
    via a cartesian(intervals) call.
    """
    for (id, start, end) in intervals:
      print(id, '  ', str(start), '  ', str(end))
      func = partial(filter_by_int, start=start, end=end)
      temp = counted.filter(func)
      
      results = temp.reduceByKey(reduce_count)
      results = results.filter(filter_count)
      result_list = results.collect()
      """ Store the lowest result in a dictionary. """
      for result in result_list:
        #print(result)
        (key, (agree, disagree, pct)) = result
        if pct_map[key] > pct or pct_map[key] < .0001:
          pct_map[key] = pct
          interval_map[key] = result

    return interval_map 
   
if __name__ == "__main__":
    context = SparkContext(master, "Congress Correlation")
    results = run(context)
    output = open('output.txt', 'w')
    for result in results:
        output.write("{0} - {1}\n".format(str(result), str(results[result])))
    output.close()

