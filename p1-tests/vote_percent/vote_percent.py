from pyspark import SparkContext

import StringIO
import csv
import datetime

votes_file = '2012-curr-full-votes.csv'
#master = "local[4]" 
master = "spark://ec2-54-158-191-221.compute-1.amazonaws.com:7077"

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
    return (tup[0], tup[1:])

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
    if left[1] == right[1]:
        agree = 1
    else:
        disagree = 1
    return (key, (agree, disagree))

def reduce_count(left, right):
    agree = left[0] + right[0] + 1
    dis = left[1] + right[1] + 1
    return (agree, dis, float(agree) / (agree + dis))

def run2(context):
    raw_votes = load_votes(context)
    bills = raw_votes.map(keyByBillId)
    bills = bills.repartition(16)
    bills = bills.sortByKey()
    joined = bills.join(bills)
    counted = joined.map(count_votes)
    counted = counted.reduceByKey(reduce_count)
    return counted.collect()

def run(context):
    raw_votes = load_votes(context)
    ''' Find all the people in this file (tuple[1])
    Then iterate through a few 'batches' at a turn, finding
    the minimum voting percentage for each person. '''
    votes = raw_votes.map(keyByPerson)
    bills = votes.map(rekeyByBillId)
    bills = bills.repartition(16)
    bills = bills.sortByKey()
    keys = votes.keys().collect()
    results = []
    for i in range(0, len(keys), 100):
        key_list = keys[i:i+10]
        #filter votes by key_list
        persons = votes.filter(lambda x: x[0] in key_list)
        persons = persons.map(rekeyByBillId)
        joined = bills.join(persons)
        counted = joined.map(count_votes)
        counted = counted.reduceByKey(reduce_count)
        results.append(counted.collect())
    return results

if __name__ == "__main__":
    context = SparkContext(master, "Congress Correlation")
    results = run2(context)
    output = open('output.txt', 'w')
    for result in results:
        output.write("{0}\n".format(str(result)))
    output.close()

