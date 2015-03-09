#!/home/vagrant/opt/spark/spark-1.2.1-bin-hadoop2.4/bin/pyspark

'''
Given a feature, such as "b_at_bats" and the period year length (yearsNumLower...yearsNumUpper), 
provide the rank of average feature score in each possible year period, in (yearsNumLower...yearsNumUpper).

Chaoren Liu @ Mar. 8, 2015
'''
from pyspark import SparkContext 
sc = SparkContext("local[4]", "ERA-checker")

features={"playerid":0, "first_name":1, "last_name":2, "weight":3, "height":4, "year":5, "round":6, "team_id":7, "league_id":8, "games_pitched":9, "games_pitched_started":10, "p_shutouts":11, "p_hits":12, "p_strikeouts":13, "p_walks":14, "p_saves":15, "p_earned_run_average":16, "b_at_bats":17, "b_runs":18, "b_hits":19, "b_doubles":20, "b_triples":21, "b_homeruns":22, "b_runs_batted_in":23, "b_stolen_bases":24, "b_strikeouts":25, "b_walks":26}

# input 
feature="b_at_bats"
DataFile="/home/vagrant/DataIntense/Project/cs516-team/p1-tests/ERA_Rank/full-mlb-player-stats.csv"
yearsNumLower=3     # the number of years considered, lower limit 
yearsNumUpper=4     # the number of years considered, upper limit 

Data=sc.textFile(DataFile).cache()

# filter out the rows with meaningful ERA data
def filterNone(x): 
    if not x.split(",")[features[feature]] in ['']:
        return True

# extract the playerID and the ERA columns
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
    DataReady=DataCleaned.join(featureData).filter(mapYeartoSlot).map(mapFormatClean).groupByKey().map(lambda (x, y): ((x[1],x[2]), mean(y), x[0])).collect()
    DataOutput=sorted(DataReady)
    
    output=open("output", 'w')
    output.write("The rank of average %s in longer than %d years and shorter than %d years period\n\n" % (feature, yearsNumLower, yearsNumUpper))
    output.write("StartYear\t\tEndYear\t\taverage of %s\t\tPlayerID\n" % feature)
    for x in DataOutput:
        ((start,end), score, playerID) = x
        output.write("(%d, %d)\t\t%.2f\t\t%s\n" % (start,end,score,playerID))

