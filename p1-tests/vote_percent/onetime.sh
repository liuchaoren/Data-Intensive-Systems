#!/bin/bash

~/ephemeral-hdfs/bin/hadoop fs -rm 2012-curr-full-votes.csv
~/ephemeral-hdfs/bin/hadoop fs -put 2012-curr-full-votes.csv 2012-curr-full-votes.csv
