#!/bin/bash

SPARK_HOME=~/spark
export PYTHONPATH=/root/cs516-team

$SPARK_HOME/bin/spark-submit --driver-memory 20g --master spark://ec2-23-20-3-44.compute-1.amazonaws.com:7077 vote_percent.py
#$SPARK_HOME/bin/spark-submit --driver-memory 5g --master spark://ec2-54-152-24-8.compute-1.amazonaws.com:7077 vote_percent.py

