#!/usr/bin/env bash

## start the HDFS and YARN daemons if they are not already running ##
if  [[ $(jps | grep "NameNode" | wc -l) -lt 1 ]] ; then
    echo "Starting DFS and YARN daemons"
    $HADOOP_HOME/sbin/start-dfs.sh && \
    $HADOOP_HOME/sbin/start-yarn.sh
fi