#!/usr/bin/env bash
set -x

input_ids_hdfs_path=/AB_NYC_2019.csv
output_hdfs_path=/hw1_out
job_name=stats
HADOOP_STREAMING_JAR=/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar

hdfs dfs -rm -r $output_hdfs_path

yarn jar $HADOOP_STREAMING_JAR \
    -files /mapper.py,/reducer.py \
    -input $input_ids_hdfs_path \
    -output $output_hdfs_path \
    -mapper "python3 mapper.py" \
    -numReduceTasks 1 \
    -reducer "python3 reducer.py"

hdfs dfs -cat $output_hdfs_path/part-00000 | head -50
