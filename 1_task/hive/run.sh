#!/usr/bin/env bash

# cd ~/Dev/hive/ # questo solo per me (Davide)
cd
hive -f $TASK_DIR/$TASK_DIR_NAME.hql
cd $MAIN_DIR