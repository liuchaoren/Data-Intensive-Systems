#!/home/vagrant/opt/spark/spark-1.2.1-bin-hadoop2.4/bin/pyspark

from pyspark import SparkContext 
import config

sc = SparkContext(config.SPARK_MASTER, "fact checking")

features={"playerid":0, "first_name":1, "last_name":2, "weight":3, "height":4, "year":5, "round":6, "team_id":7, "league_id":8, "games_pitched":9, "games_pitched_started":10, "p_shutouts":11, "p_hits":12, "p_strikeouts":13, "p_walks":14, "p_saves":15, "p_earned_run_average":16, "b_at_bats":17, "b_runs":18, "b_hits":19, "b_doubles":20, "b_triples":21, "b_homeruns":22, "b_runs_batted_in":23, "b_stolen_bases":24, "b_strikeouts":25, "b_walks":26}

feature="b_at_bats"
DataFile="s3n://" + config.S3_AWS_ACCESS_KEY_ID + ":" + config.S3_AWS_SECRET_ACCESS_KEY + "@cs516-fact-check/full-mlb-player-stats.csv"

data=sc.textFile(DataFile).cache()
def getFeature(x):
    xl=x.split(',')
    return (xl[features['playerid']],(xl[features['year']],xl[features[feature]]))
def getMean(x):
    id=x[0]
    yearDict={}
    everage=[]
    for each in x[1]:
	if len(each[1])==0:
	    if not int(each[0]) in yearDict: 
	    	yearDict[int(each[0])]=0
	else:
	    if not int(each[0]) in yearDict:
                yearDict[int(each[0])]=float(each[1])
	    else:
		yearDict[int(each[0])]+=float(each[1])
    for i in yearDict:
        for j in yearDict:
            if (j-i)<3:
               continue
	    sum=0
	    cnt=0
            for k in range(i,j+1):
		if k in yearDict:
		    sum+=yearDict[k]
		    cnt+=1
	    everage.append(float(sum)/(cnt))
    max=-1
    for each in everage:
	if each>max:
	    max=each
    return (id,max)
def getImprovement(x):
    id=x[0]
    yearDict={}
    improvement=[]
    for each in x[1]:
        if len(each[1])==0:
	    if not int(each[0]) in yearDict:
	        yearDict[int(each[0])]=0
	else:
	    if not int(each[0]) in yearDict:
	        yearDict[int(each[0])]=float(each[1])
	    else:
		yearDict[int(each[0])]+=float(each[1])
    for i in yearDict:
	for j in yearDict:
	    if (j-i)<5:
		continue
            if id=='raineti01':
	        print i,j
            improvement.append((float(yearDict[j])-float(yearDict[i]))/(j-i+1)/(yearDict[i]+1))
    max=-1
    for each in improvement:
	if each>max:
	    max=each
    return (id,max)
def filter100(x):
    sum=0
    for each in x[1]:
        if len(each[1])>0:
            sum+=float(each[1])
    return sum>100
def getKey(x):
    return x[1]
playerdata=data.map(getFeature).groupByKey()
ret=playerdata.map(getMean).top(5,getKey)
output=open('output','w')
output.write(str(ret)+'\n')
ret=playerdata.filter(filter100).map(getImprovement).top(1,getKey)
output.write(str(ret))
output.close()
