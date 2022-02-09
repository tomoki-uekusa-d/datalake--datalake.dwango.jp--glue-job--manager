#!/bin/bash

if [ $# -ne 1 ]; then
  echo "Invalid number of arguments"
  echo "    Set 'json_path', Check aws glue create-job --generate-cli-skeleton"
  exit 1
fi

JSON_PATH=$1
ABS_JSON_PATH=$(cd $(dirname $JSON_PATH); pwd)/$(basename $JSON_PATH)

aws glue create-job --cli-input-json file://$ABS_JSON_PATH
