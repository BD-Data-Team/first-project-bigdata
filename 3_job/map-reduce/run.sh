# !/usr/bin/env bash

# echo "Staarting task 1"
# hadoop jar $HADOOP_STREAMING_JAR_PATH \
#             -mapper $JOB_DIR/mapper1.py -reducer $JOB_DIR/reducer1.py \
#             -input $HDFS_DATASET_PATH -output "${HDFS_FULL_JOB_OUTPUT_DIR_PATH}_0"

# if [[ $? -gt 0 ]] ; then
#     echo "Task 1 failed"
#     hdfs dfs -rm -r $"${HDFS_FULL_JOB_OUTPUT_DIR_PATH}_0"
#     exit 1
# fi

# echo "Task 1 completed successfully, starting task 2"
# hadoop jar $HADOOP_STREAMING_JAR_PATH \
#         -mapper $JOB_DIR/mapper2.py -reducer $JOB_DIR/reducer2.py \
#         -input "${HDFS_FULL_JOB_OUTPUT_DIR_PATH}_0" -output "${HDFS_FULL_JOB_OUTPUT_DIR_PATH}_1" 

# hdfs dfs -rm -r "${HDFS_FULL_JOB_OUTPUT_DIR_PATH}_0"

# if [[ $? -gt 0 ]] ; then
#     echo "Task 2 failed"
#     hdfs dfs -rm -r "${HDFS_FULL_JOB_OUTPUT_DIR_PATH}_1"
#     exit 1
# fi

# echo "Task 2 completed successfully, starting task 3"
# hadoop jar $HADOOP_STREAMING_JAR_PATH \
#         -mapper $JOB_DIR/mapper2.py -reducer $JOB_DIR/reducer2.py \
#         -input "${HDFS_FULL_JOB_OUTPUT_DIR_PATH}_1" -output "${HDFS_FULL_JOB_OUTPUT_DIR_PATH}_2"

# hdfs dfs -rm -r "${HDFS_FULL_JOB_OUTPUT_DIR_PATH}_1"


# if [[ $? -gt 0 ]] ; then
#     echo "Task 3 failed"
#     hdfs dfs -rm -r "${HDFS_FULL_JOB_OUTPUT_DIR_PATH}_2"
#     exit 1
# fi

echo "Task 3 completed successfully, starting task 4"
hadoop jar $HADOOP_STREAMING_JAR_PATH \
        -mapper $JOB_DIR/mapper3.py -reducer $JOB_DIR/reducer3.py \
        -input "${HDFS_FULL_JOB_OUTPUT_DIR_PATH}_2" -output $HDFS_FULL_JOB_OUTPUT_DIR_PATH

# hdfs dfs -rm -r "${HDFS_FULL_JOB_OUTPUT_DIR_PATH}_2"

if [[ $? -gt 0 ]] ; then
    echo "Task 2 failed"
    hdfs dfs -rm -r $HDFS_FULL_JOB_OUTPUT_DIR_PATH
    exit 1
fi