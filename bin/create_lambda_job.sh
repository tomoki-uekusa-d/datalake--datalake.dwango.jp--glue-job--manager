#!/bin/bash

if [ $# -ne 2 ]; then
  echo "Invalid number of arguments"
  echo "    Set 'function_name' and 'job_name'"
  exit 1
fi

FUNCTION_NAME=$1
JOB_NAME=$2

SCRIPT_DIR=$(cd $(dirname $0); pwd)

pushd $SCRIPT_DIR/../lambda
  zip -r lambda.zip .
  aws lambda update-function-configuration --function-name $FUNCTION_NAME --environment Variables={JOB_NAME=\'$JOB_NAME\'}
  aws lambda update-function-code --function-name $FUNCTION_NAME --zip-file fileb://lambda.zip
  rm -v lambda.zip
popd
