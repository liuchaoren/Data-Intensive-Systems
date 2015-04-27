#!/bin/bash

SPARK_HOME=~/spark
export PYTHONPATH=/root/cs516-team
MASTER=spark://ec2-23-22-131-140.compute-1.amazonaws.com:7077

$SPARK_HOME/bin/spark-submit --driver-memory 5g --master $MASTER --conf spark.eventLog.enabled=true vote_group_by.py
#$SPARK_HOME/bin/spark-submit --driver-memory 5g --master spark://ec2-54-152-24-8.compute-1.amazonaws.com:7077 vote_percent.py

