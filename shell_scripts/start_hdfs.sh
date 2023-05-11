#!/usr/bin/env bash

## start the HDFS and YARN daemons ##

echo "Starting DFS and YARN daemons"
$HADOOP_HOME/sbin/start-dfs.sh && \
$HADOOP_HOME/sbin/start-yarn.sh