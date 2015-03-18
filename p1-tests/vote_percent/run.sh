#!/bin/bash

SPARK_HOME=~/spark

$SPARK_HOME/bin/spark-submit --driver-memory 5g --master spark://ec2-54-226-211-25.compute-1.amazonaws.com:7077 vote_percent.py
#$SPARK_HOME/bin/spark-submit --driver-memory 5g --master spark://ec2-54-152-24-8.compute-1.amazonaws.com:7077 vote_percent.py

