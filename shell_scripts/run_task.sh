#!/usr/bin/env bash

# $1 is the task number
# $2 is the implementation name

#check the number of arguments
if [[ $# -ne 2 ]] ; then
    echo "Usage: ./run_task.sh <task_number> <implementation_name>"
    exit 1
fi

# get the directory of the current script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# initialize the environment variables
. $SCRIPT_DIR/setup_env.sh

# get the task directory name
TASK_DIR_NAME=$1$TASK_SUFFIX

HDFS_TASK_OUTPUT_DIR_PATH=$HDFS_OUTPUT_DIR_PATH/$TASK_DIR_NAME
# get the task output directory path
HDFS_FULL_TASK_OUTPUT_DIR_PATH=$HDFS_OUTPUT_DIR_PATH/$TASK_DIR_NAME/$2


if [[ $(hdfs dfs -ls $HDFS_OUTPUT_DIR_PATH/$TASK_DIR_NAME | grep $2 | wc -l) -gt 0 ]] ; then
    echo "Removing the output directory $HDFS_FULL_TASK_OUTPUT_DIR_PATH from HDFS"
    hdfs dfs -rm -r $HDFS_FULL_TASK_OUTPUT_DIR_PATH
fi

TASK_DIR=$MAIN_DIR/$TASK_DIR_NAME/$2
. $TASK_DIR/run.sh