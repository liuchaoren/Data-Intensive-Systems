from pyspark import SparkContext

import StringIO
import csv
import datetime
import permute.interval as interval
import pandas

votes_file = '2012-curr-full-votes.csv'
#master = "local[4]" 
master = "spark://ec2-54-163-106-178.compute-1.amazonaws.com:7077"

def load_votes(context):
    votes_data = context.textFile(votes_file, use_unicode=False).cache()
    return votes_data

def keyByBillId(line):
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

def map_by_interval(((key, (agree, disagree, vote_date)), (id, start, end))):
    newKey = key + ":" + str(start) + ":" + str(end)
    return (newKey, (agree, disagree))

def filter_by_interval(((key, (agree, disagree, vote_date)), (id, start, end))):
    if vote_date >= start and vote_date <= end:
        return True
    return False

def reduce_count(left, right):
    agree = left[0] + right[0]
    dis = left[1] + right[1]
    return (agree, dis)

def filter_join((key, (left, right))):
  if left[0] < right[0]:
     return True
  return False

def run2(context):
    raw_votes = load_votes(context)
    intervals = interval.interval_set('1/1/2011', '1/1/2014', freq='D', max_delta=pandas.Timedelta(days=120))
    bills = raw_votes.map(keyByBillId)
    joined = bills.join(bills, 16)
    joined = joined.filter(filter_join)
    counted = joined.map(count_votes)
    #now join in the intervals? 
    #or glom things together? 
    intervals_rdd = context.parallelize(intervals)
    interval_cart = counted.cartesian(intervals_rdd)
    interval_cart = interval_cart.filter(filter_by_interval)
    interval_cart = interval_cart.map(map_by_interval)
    counted = interval_cart.reduceByKey(reduce_count)
    return counted.collect()

if __name__ == "__main__":
    context = SparkContext(master, "Congress Correlation")
    results = run2(context)
    output = open('output.txt', 'w')
    for result in results:
        output.write("{0}\n".format(str(result)))
    output.close()

