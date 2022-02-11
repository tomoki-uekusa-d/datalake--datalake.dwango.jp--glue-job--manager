#!/bin/bash

if [ $# -ne 1 ]; then
  echo "Invalid number of arguments"
  echo "    Set 'job_name', Check aws glue get-jobs"
  exit 1
fi

JOB_NAME=$1

set -eu

RUN_ID=$(aws glue start-job-run --job-name $JOB_NAME | grep JobRunId | awk '{print $NF}' | sed -e 's/"//g')

echo "RUN_ID : $RUN_ID"
echo "Tailing logs by following command"
echo "    % aws logs tail --follow /aws-glue/jobs/output --filter-pattern $RUN_ID"
echo "    % aws logs tail --follow /aws-glue/jobs/error --filter-pattern $RUN_ID"

echo "-------------------------------------"
while true
do
  STATUS=$(aws glue get-job-run --job-name $JOB_NAME --run-id $RUN_ID | grep JobRunState | awk '{print $NF}' | sed -e 's/"//g' -e 's/,//g')
  printf "\rSTATUS: $STATUS..."
  if [ "$STATUS" != "RUNNING" ]; then
    break
  fi
  sleep 5
done
echo "\n-------------------------------------"

aws glue get-job-run --job-name $JOB_NAME --run-id $RUN_ID 
