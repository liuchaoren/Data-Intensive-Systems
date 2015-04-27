from pyspark import SparkContext
from functools import partial
from collections import defaultdict
import StringIO
import csv
import datetime
import permute.interval as interval
import pandas
import numpy as np

votes_file = '2012-curr-full-votes.csv'
#master = "local[4]"
master = "spark://ec2-54-83-184-241.compute-1.amazonaws.com:7077"

def load_votes(context):
    votes_data = context.textFile(votes_file, use_unicode=False).cache()
    return votes_data


def keyByBillId(line):
    """
    Key by the bill id to be used in the self join step. The tuple looks like:
    (bill_id, (person_id, date, vote))
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
    return (tup[0].strip(), [tup[1].strip(), tup[6], tup[2].strip()])

def count_votes((key, joined_tuple)):
    """
    Map function that counts the votes between two entries.
    Called on a self-joined bills entry, so the incoming tuple looks like:
    (bill_id, ((person_id, date, vote), (person_id, date, vote))
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
    return (agree, dis)


def filter_count((key, (agree, dis))):
    """
    Filter any entries that don't have at least 50 votes.
    This is also needed because some entries get filtered out completely
    and so the previous reduce_count stage is not called for some tuples,
    resulting in the third entry in this tuple being a date object instead
    of a percent.
    """
  
    if agree + dis < 50:
        return False
    else:
        return True


def filter_join((key, (left, right))):
    """
    Avoids duplicate calculations/entries
    ex: ("p_id_1:p_id_2", "p_id_2:p_id_1"), removes the right one.
    """
    if left[0] < right[0]:
        return True
    return False


def filter_by_int(((key, (agree, disagree, vote_date)), (id, start, end))):
    if vote_date >= start and vote_date <= end:
        return True
    return False

def filter_by_int_big(((key,(agree, disagree)), (id, start, end))):
    if key[1] >=start and key[2] <= end:
        return True
    else:
        return False
        
def key_by_interval_and_person(((key, (agree, disagree, vote_date)), (id, start, end))):
    return ((key, start, end), (agree, disagree))

def key_by_interval_and_person_big(((key, (agree, disagree)), (id, start, end))):
    return ((key[0], start, end), (agree, disagree))

def key_by_2persons((key, (agree, disagree))):
    newKey = key[0]
    return (newKey, (key[1], key[2], agree, disagree))

def min_percent((start1, end1, agree1, disagree1), (start2, end2, agree2, disagree2)):
    if agree1/float(disagree1+agree1) > agree2/float(disagree2+agree2):
        return (start2, end2, agree2, disagree2)
    else:
        return (start1, end1, agree1, disagree1)

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
    big_intervals = interval.interval_set('1/1/2012', '1/1/2014', freq='15D', max_delta=pandas.Timedelta(days=120))
    #small_intervals = interval.interval_set('1/1/2012', '1/1/2014', freq='15D', max_delta=pandas.Timedelta(days=15))
    bills = raw_votes.map(keyByBillId)
    joined = bills.join(bills, 24)
    joined = joined.filter(filter_join)
    counted = joined.map(count_votes)

    #small_ints_rdd = context.parallelize(small_intervals)
    big_ints_rdd = context.parallelize(big_intervals)
    votes_intervals = counted.cartesian(big_ints_rdd)
    votes_intervals = votes_intervals.filter(filter_by_int)
    votes_intervals = votes_intervals.map(key_by_interval_and_person)
    results = votes_intervals.reduceByKey(reduce_count)

    results = results.filter(filter_count).map(key_by_2persons)
    result_list = results
    #result_list = results.reduceByKey(min_percent)
    return result_list

if __name__ == "__main__":
    context = SparkContext(master, "Congress Correlation")
    results = run(context).collect()
    with open('output.csv', 'w') as csvfile:
      writer = csv.writer(csvfile)
      for result in results:
        (persons, (start, end, agree, disagree)) = result
        person_split = persons.split(":")
        writer.writerow([person_split[0], person_split[1], str(start), str(end), str(agree), str(disagree), str((agree / (agree + disagree)))])
