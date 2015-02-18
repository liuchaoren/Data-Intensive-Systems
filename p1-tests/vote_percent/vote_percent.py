from pyspark import SparkContext

import StringIO
import csv
import datetime

#votes_file = 'full_votes.csv'

votes_file = '2012-curr-full-votes.csv'

master = "local[4]" 
def load_votes(context):
    votes_data = context.textFile(votes_file, use_unicode=False).cache()
    return votes_data

def parseVote(line):
    df = '%Y-%m-%d'    
    input = StringIO.StringIO(line)
    reader = csv.reader(input)
    tup = reader.next()
    if tup:
      tup[6] = datetime.datetime.strptime(tup[6], df).date()
    return (tup[0], tup[1:])

def count_votes(joined_tuple):
    key = joined_tuple[0]
    joined= joined_tuple[1]
    left = joined[0]
    right = joined[1]
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
    agree = left[0] + right[0]
    dis = left[1] + right[1]
    return (agree, dis, float(agree) / (agree + dis))

def run(context):
    votes = load_votes(context)
    votes = votes.map(parseVote)
    joined = votes.join(votes)
    counted = joined.map(count_votes)
    counted = counted.reduceByKey(reduce_count)
    return counted
    #return votes

if __name__ == "__main__":
    context = SparkContext(master, "Congress Correlation")
    results = run(context)
    result_list = results.collect()
    output = open('output.txt', 'w')
    output.write("i am a file.\n")
    for result in result_list:
        output.write("{0}\n".format(str(result)))
    output.close()

