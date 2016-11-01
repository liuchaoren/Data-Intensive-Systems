#!/bin/bash

SPARK_HOME=~/spark
export PYTHONPATH=/root/cs516-team
MASTER=spark://ec2-54-83-184-241.compute-1.amazonaws.com:7077

#$SPARK_HOME/bin/spark-submit --driver-memory 13g --master $MASTER --conf spark.eventLog.enabled=true vote_group_by.py
$SPARK_HOME/bin/spark-submit --driver-memory 13g --master $MASTER --conf spark.eventLog.enabled=True vote_percent.py

