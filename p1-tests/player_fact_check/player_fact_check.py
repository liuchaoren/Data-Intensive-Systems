from pyspark import SparkContext

import StringIO
import csv
import datetime

data_file = '../../../full-mlb-player-stats.csv'
global Given
global xhrsb
master = "local[4]" 

def load_data(context):
    data = context.textFile(data_file, use_unicode=False).cache()
    return data

def parsePlayer(line):
    linel=line.split(',')
    id=linel[0]
    hr=linel[-5]
    sb=linel[-3]
    if len(hr)==0:
	hr=0
    if len(sb)==0:
	sb=0
    return (id,(int(hr),int(sb))) 

def Sum(a,b):
    return (a[0]+b[0],a[1]+b[1])

def compare3040(line):
    id=line[0]
    hrsb=line[1]
    hr=hrsb[0]
    sb=hrsb[1]
    if hr>=30 and sb>=30:
        thirty=1
    else:
	thirty=0
    if hr>=40 and sb>=40:
        forty=1
    else:
	forty=0
    return (forty,thirty)   

def countPlayer(a,b):
    return (a[0]+b[0],a[1]+b[1])

def checkOver3040(context):
    data = load_data(context)
    return data.map(parsePlayer).reduceByKey(Sum).map(compare3040).reduce(countPlayer)

def findPlayerX(line):
    global Given
    linel=line.split(',')
    if linel[0]==Given.value[0] and linel[5]==Given.value[1] and linel[6]==Given.value[2]:
        return 1
    else:
	return 0
def compareX(line):
    global Given
    global xhrsb
    linel=line.split(',')
    if linel[5]!=Given.value[1] or linel[6]!=Given.value[2]:
	return False
    if linel[-5]>=xhrsb.value[0] and linel[-3]>=xhrsb.value[1]:
	return True
    else:
	return False
def checkOverPlayerX(context,Player,Year,Round):
    global Given
    global xhrsb
    data = load_data(context)
    Given=context.broadcast((Player,Year,Round))
    PlayerX=data.filter(findPlayerX)
    PlayerXl=PlayerX.collect()[0].split(',')
    xhrsb = context.broadcast((PlayerXl[-5],PlayerXl[-3]))
    return data.filter(compareX).count()

if __name__ == "__main__":
    Player="aasedo01"
    Year="1977"
    Round="1"
    
    context = SparkContext(master, "player fact check")
     
    results1 = checkOver3040(context)
    results2 = checkOverPlayerX(context,Player,Year,Round)
    
    output = open('output.txt', 'w')
    output.write("Baseball fact check result:\n")
    output.write("The number of people over 40 home runs and 40 base steals: "+str(results1[0])+"\n")
    output.write("The number of people over 30 home runs and 30 base steals: "+str(results1[1])+"\n")
    output.write("The number of people who has more home runs and base steals than "+Player+" in the round "+Round+" of "+Year+": "+str(results2))
    output.close()

