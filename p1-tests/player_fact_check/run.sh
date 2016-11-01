#!/bin/bash

SPARK_HOME=/home/vagrant/spark-1.2.1-bin-hadoop2.4

$SPARK_HOME/bin/spark-submit --driver-memory 3g --master local[4] player_fact_check.py
