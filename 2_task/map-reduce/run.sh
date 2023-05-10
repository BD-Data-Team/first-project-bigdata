#!/usr/bin/env bash

echo Staarting task 1
hadoop jar $HADOOP_STREAMING_JAR_PATH \
            -mapper $TASK_DIR/mapper1.py -reducer $TASK_DIR/reducer1.py \
            -input $HDFS_DATASET_PATH -output "${HDFS_FULL_TASK_OUTPUT_DIR_PATH}_0"

if [[ $? -gt 0 ]] ; then
    echo "Task 1 failed"
    hdfs dfs -rm -r $"${HDFS_FULL_TASK_OUTPUT_DIR_PATH}_0"
    exit 1
fi

echo "Task 1 completed successfully, starting task 2"
hadoop jar $HADOOP_STREAMING_JAR_PATH \
        -mapper $TASK_DIR/mapper2.py -reducer $TASK_DIR/reducer2.py \
        -input "${HDFS_FULL_TASK_OUTPUT_DIR_PATH}_0" -output $HDFS_FULL_TASK_OUTPUT_DIR_PATH

hdfs dfs -rm -r $"${HDFS_FULL_TASK_OUTPUT_DIR_PATH}_0"

exit 0