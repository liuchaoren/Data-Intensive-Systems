#!/home/vagrant/opt/spark/spark-1.2.1-bin-hadoop2.4/bin/pyspark

'''
The rank of a player in earned runs average
Bug: different workers use their own global counter1. therefore, the rank may be the rank in one worker but the universal rank

Chaoren Liu @ Feb. 21, 2015
'''
playerID="linceti01"

from pyspark import SparkContext 
DataFile="full-mlb-player-stats.csv"
sc = SparkContext("local", "Stats")
Data=sc.textFile(DataFile).cache()

# filter out the rows with meaningful ERA data
def filterfunc(x): 
    if not x.split(",")[16] in ['', '0']:
        return True

# extract the playerID and the ERA columns
def mapfuncValue(x):
    xstructed=x.split(",")
    (playerID, PERA)=(xstructed[0],float(xstructed[16]))
    return (playerID, PERA)

# count the numbers of the lines of a player
def mapfuncCount(x):
    xstructed=x.split(",")
    playerID =xstructed[0]
    return (playerID, 1)


def mapfuncAve((pid, (total, count))):
    return (total/count, pid)
    
counter1=0
def g((i,x)):
    global counter1
    counter1=counter1+1
    for j in x:
        if j == playerID:
            print "The ERA of player " + playerID + " is %f" % i
            print "The ERA rank is %d" % counter1

DataFiltered=Data.filter(filterfunc)
DataCleaned=DataFiltered.map(mapfuncValue).reduceByKey(lambda x, y: x+y)
DataCounter=DataFiltered.map(mapfuncCount).reduceByKey(lambda x, y: x+y)
DataCleaned=DataCleaned.join(DataCounter).map(mapfuncAve).groupByKey().sortByKey()
DataCleaned.foreach(g)

