#!/usr/bin/env bash

CURRENT_DIR=$(pwd)

cd ~/Dev/hive/ # questo solo per me (Davide)
# cd
hive --hiveconf dataset=$HDFS_DATASET_PATH \
     --hiveconf output_dir=$HDFS_FULL_JOB_OUTPUT_DIR_PATH \
     --hiveconf input_dir=$HDFS_INPUT_DIR_PATH \
     -f $JOB_DIR/$JOB_DIR_NAME.hql

cd $CURRENT_DIR