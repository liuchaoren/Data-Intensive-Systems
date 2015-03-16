#!/home/vagrant/opt/spark/spark-1.2.1-bin-hadoop2.4/bin/pyspark

'''
The rank of a player in earned runs average

Chaoren Liu @ Feb. 21, 2015
'''
from pyspark import SparkContext 
sc = SparkContext("local[4]", "ERA-checker")

# input 
playerID="linceti01"
DataFile="../../../full-mlb-player-stats.csv"

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

# 

if __name__ == "__main__":
    DataCleaned=Data.filter(filterfunc).map(mapfuncValue).reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1])
    ERAtarget=DataCleaned.lookup(playerID)[0]
    rank=DataCleaned.filter(lambda x: round(x[1], 3) < round(ERAtarget, 3)).count()
    output=open("output", 'w')
    output.write("The ERA of the player %s is %8.3f\n" % (playerID, ERAtarget))
    output.write("The rank of the player %s in ERA is %d\n" % (playerID, rank))
    output.close()


