#!/bin/bash
file_path1=$1
file_path2=$2
s3_base_path=$3
num=$4
# Iterate over n time in this shell script and run the python script
for ((i=1; i<=${num}; i++))
do
  echo "Iteration: $i"
  aws s3 cp $file_path1 $s3_base_path/$i/$(date +%s%3)/ &
  aws s3 cp $file_path2 $s3_base_path/$i/$(date +%s%3)/ &
done

wait



