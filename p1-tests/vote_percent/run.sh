#!/bin/bash

SPARK_HOME=~/spark
export PYTHONPATH=/root/cs516-team

$SPARK_HOME/bin/spark-submit --driver-memory 5g --master spark://ec2-54-167-24-226.compute-1.amazonaws.com:7077 --conf spark.eventLog.enabled=true vote_percent_cross_Chaoren.py
#$SPARK_HOME/bin/spark-submit --driver-memory 5g --master spark://ec2-54-152-24-8.compute-1.amazonaws.com:7077 vote_percent.py

