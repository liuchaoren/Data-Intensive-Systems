#!/home/vagrant/opt/spark/spark-1.2.1-bin-hadoop2.4/bin/pyspark

'''
count the number of yes votes by persion_a in 2012

Chaoren Liu @ Feb. 14, 2015
'''

from pyspark import SparkContext
DataFile = "votes.csv" 
sc = SparkContext("local","Count Votes")
Data=sc.textFile(DataFile).cache()

def yesfilter(s):
    endpoint = s.find(",")
    vote=s[:endpoint]
    return ",2012," in s and vote in ['Yea', 'Aye']

NumYesA=Data.filter(yesfilter).count()
outputfile=open("output_VoteCounts",'w')
outputfile.write("The number of YES votes by persion_a in the year 2012 is %d\n" % NumYesA)
