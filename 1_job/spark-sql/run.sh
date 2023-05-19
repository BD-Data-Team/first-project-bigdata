#!/usr/bin/env bash

spark-submit --master yarn \
        $JOB_DIR/1_job.py \
        --input_path $HDFS_DATASET_PATH \
        --output_path $HDFS_FULL_JOB_OUTPUT_DIR_PATH