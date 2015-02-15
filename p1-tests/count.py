from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("count_disagree").setMaster("local")
sc = SparkContext(conf=conf)
testFile="../test-data/votes.csv"
testData=sc.textFile(testFile).cache()
#num=testData.count()
def disagree(line):
    linel=line.split(",")
    if len(linel[0])>=7 or len(linel[1])>=7:
	return False
    elif 'e' in linel[0] and 'N' in linel[1]:
	return True
    elif 'N' in linel[0] and 'e' in linel[1]:
	return True
    else:
	return False
num=testData.filter(disagree).count()
print "The number of times the two people disagreed on votes is",num
