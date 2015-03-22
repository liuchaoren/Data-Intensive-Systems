#!/home/vagrant/opt/spark/spark-1.2.1-bin-hadoop2.4/bin/pyspark

'''
Chaoren Liu @ Mar. 22, 2015
'''

from pyspark import SparkContext 

#import config
#master = config.SPARK_MASTER
master = "local[4]" 
sc = SparkContext(master, "MLB-checker")

features={"playerid":0, "first_name":1, "last_name":2, "weight":3, "height":4, "year":5, "round":6, "team_id":7, "league_id":8, "games_pitched":9, "games_pitched_started":10, "p_shutouts":11, "p_hits":12, "p_strikeouts":13, "p_walks":14, "p_saves":15, "p_earned_run_average":16, "b_at_bats":17, "b_runs":18, "b_hits":19, "b_doubles":20, "b_triples":21, "b_homeruns":22, "b_runs_batted_in":23, "b_stolen_bases":24, "b_strikeouts":25, "b_walks":26}

# input 
feature="b_at_bats"
#DataFile="s3n://" + config.S3_AWS_ACCESS_KEY_ID + ":" + config.S3_AWS_SECRET_ACCESS_KEY  + "@cs516-fact-check/full-mlb-player-stats.csv"
DataFile="/home/vagrant/DataIntense/Project/cs516-team/p1-tests/ERA_Rank/full-mlb-player-stats.csv"
yearsNumLower=3     # the number of years considered, lower limit 
yearsNumUpper=4     # the number of years considered, upper limit 
playerID="rolliji01"

Data=sc.textFile(DataFile, use_unicode=False).cache()

# filter out the rows with meaningful ERA data
def filterNone(x): 
    if not x.split(",")[features[feature]] in ['']:
        return True

def mapYear(x):
    xstructed=x.split(",")
    (playerID, year)=(xstructed[features["playerid"]], int(xstructed[features["year"]]))
    return (playerID, year)

# extract the year and the feature
def mapFeature(x):
    xstructed=x.split(',')
    (playerID, (year, featureData)) = (xstructed[features["playerid"]], (int(xstructed[features["year"]]), float(xstructed[features[feature]])))
    return (playerID, (year, featureData))
    
# 
def mapYeartoSlot(x):
    (name, ((start, end), (year, data))) = x
    if year >= start and year <= end:
        return True 

# min 
def min(iterable):
    minyear=10000
    for i in iterable:
        if i < minyear:
            minyear=i
    return minyear


# max
def max(iterable):
    maxyear=0
    for i in iterable:
        if i > maxyear:
            maxyear=i
    return maxyear

# mean
def mean(iterable):
    counter=0
    sum=0.
    for i in iterable:
        sum=sum+i
        counter=counter+1
    return sum/counter
        

# period generator
def period_set(yearMin, yearMax, minStep, maxStep):
    return [(yearStart, yearEnd) for yearStart in range(yearMin, yearMax+1) for yearEnd in range(yearMin, yearMax+1) if (yearEnd - yearStart + 1) >= minStep and (yearEnd - yearStart + 1) <= maxStep] 

# 
def mapFormatClean(x):
    (name, ((start, end), (year, data))) = x
    return ((name, start, end), data)
    

# print  
def p(x):
    print x

if __name__ == "__main__":
    removedNone=Data.filter(filterNone)
    DataCleaned=removedNone.map(mapYear).groupByKey().map(lambda (x,y): (x, period_set(min(y), max(y), yearsNumLower, yearsNumUpper))).flatMapValues(lambda x: x)
    featureData=removedNone.map(mapFeature)
    DataReady=DataCleaned.join(featureData).filter(mapYeartoSlot).map(mapFormatClean).groupByKey().map(lambda (x, y): ((x[1],x[2]), mean(y), x[0]))
    DataPlayer=DataReady.filter(lambda x: x[2] == playerID).takeOrdered(1, lambda x: -x[1])
    MaxPlayer=DataPlayer[0][1]
    DataOutput=DataReady.filter(lambda x: x[1] > MaxPlayer)
    DataOutputNum=DataOutput.count()
    DataOutputReady=DataOutput.takeOrdered(DataOutputNum, lambda x: -x[1])
    BetterPlayerID=DataOutput.map(lambda x: x[2]).distinct().collect()
    
    output=open("output", 'w')
    output.write("Player %s achieved an anverage b_at_bats of %.3f in the year slot of (%d, %d), which is his highest score in three-year time slots\n\n" % (playerID, MaxPlayer, DataPlayer[0][0][0], DataPlayer[0][0][1]))
    output.write("Only %d players can beat him in the history and they are " % len(BetterPlayerID))
    output.write(', '.join(BetterPlayerID)+'\n\n\n')
    output.write("PlayerID\t\t\t(StartYear,EndYear)\t\taverage b_at_bats score\n")
    for eachrecord in DataOutputReady:
        output.write("%s\t\t\t(%d,%d)\t\t\t\t%.3f\n" %  (eachrecord[2], eachrecord[0][0], eachrecord[0][1], eachrecord[1]))
