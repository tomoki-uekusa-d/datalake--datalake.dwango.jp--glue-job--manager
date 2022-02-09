#!/bin/bash

if [ $# -ne 2 ]; then
  echo "Invalid number of arguments"
  echo "    Set env 'test or prod' and 'job_name'"
  exit 1
fi

DEPLOY_ENV=$1
JOB_NAME=$2

SCRIPT_DIR=$(cd $(dirname $0); pwd)

if [ "$DEPLOY_ENV" = "test" ]; then
  AWS_BUCKET="test-glue.in.dwango.jp"
  aws s3 sync $SCRIPT_DIR/../src/$AWS_BUCKET/etl_scripts/new_arrivals/$JOB_NAME s3://$AWS_BUCKET/etl_scripts/new_arrivals/$JOB_NAME
  exit 0
fi

if [ "$DEPLOY_ENV" = "prod" ]; then
  AWS_BUCKET="glue.in.dwango.jp"
  aws s3 sync $SCRIPT_DIR/../src/$AWS_BUCKET/etl_scripts/new_arrivals/$JOB_NAME s3://$AWS_BUCKET/etl_scripts/new_arrivals/$JOB_NAME
  exit 0
fi

echo "Do nothing..."
echo "    Set 'test' or 'prod'"
exit 1
