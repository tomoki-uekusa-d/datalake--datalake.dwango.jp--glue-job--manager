# datalake--datalake.dwango.jp--glue-job---manager

Development datalake--datalake.dwango.jp--glue-job

## Unit Tests

### testing on local (require install spark on local)

```console
% pipenv run test
```

### testing on Docker (require install docker)

```console
% docker build -t glue-development .
% pipenv run test-docker
```

### Tips

Deploy all

```console
find glue/cli-input-json -type f | grep -v 'test' | awk -F'/' '{print $NF}' | awk -F'.' '{print $1}' | xargs -L1 -I{} bash -c 'bash bin/deploy_glue_script.sh prod {}'
```

Execute Single Job

```console
% bash bin/execute_glue_job.sh init_new_arrivals_fact_animelomix_android_voice_dataset
RUN_ID : jr_a65551a38f92edca7be01f735747a0bb75938b38623df0a732e1fac4ebc58f83
JOB_URL : https://ap-northeast-1.console.aws.amazon.com/gluestudio/home?region=ap-northeast-1#/job/init_new_arrivals_fact_animelomix_android_voice_dataset/run/jr_a65551a38f92edca7be01f735747a0bb75938b38623df0a732e1fac4ebc58f83
Tailing logs by following command
aws logs tail --filter '?ERROR ?WARN ?INFO' --follow /aws-glue/jobs/output --log-stream-names jr_a65551a38f92edca7be01f735747a0bb75938b38623df0a732e1fac4ebc58f83
aws logs tail --filter '?ERROR ?WARN ?INFO' --follow /aws-glue/jobs/error --log-stream-names jr_a65551a38f92edca7be01f735747a0bb75938b38623df0a732e1fac4ebc58f83
STATUS: SUCCEEDED
{
    "JobRun": {
        "Id": "jr_a65551a38f92edca7be01f735747a0bb75938b38623df0a732e1fac4ebc58f83",
        "Attempt": 0,
        "JobName": "init_new_arrivals_fact_animelomix_android_voice_dataset",
        "StartedOn": "2023-03-31T16:35:22.926000+09:00",
        "LastModifiedOn": "2023-03-31T16:55:29.420000+09:00",
        "CompletedOn": "2023-03-31T16:55:29.420000+09:00",
        "JobRunState": "SUCCEEDED",
        "PredecessorRuns": [],
        "AllocatedCapacity": 10,
        "ExecutionTime": 1199,
        "Timeout": 2880,
        "MaxCapacity": 10.0,
        "WorkerType": "G.1X",
        "NumberOfWorkers": 10,
        "LogGroupName": "/aws-glue/jobs",
        "GlueVersion": "2.0"
    }
}
```

Use DataGenerator

```console
% python3 -m bin.data_generator -h
usage: data_generator.py [-h] -jn JOB_NAME -sm START_MONTH -em END_MONTH [-b BUCKET] [-s SITE] [-c CORNER] [-se] [-dr] [-sms SKIP_MONTHS]

optional arguments:
  -h, --help            show this help message and exit
  -jn JOB_NAME, --job_name JOB_NAME
                        job name
  -sm START_MONTH, --start_month START_MONTH
                        start month YYYYMM
  -em END_MONTH, --end_month END_MONTH
                        end month YYYYMM
  -b BUCKET, --bucket BUCKET
                        bucket
  -s SITE, --site SITE  site
  -c CORNER, --corner CORNER
                        corner
  -se, --skip_exists    skip exists
  -dr, --dryrun         dryrun
  -sms SKIP_MONTHS, --skip_months SKIP_MONTHS
                        skipped months e.g. "202209,202208"

## e.g.

% python3 -m bin.data_generator -jn init_new_arrivals_fact_melody_android_amuse_dataset -s melody_android -c amuse -sm 201812 -em 201801 -sms 201810,201805,201804,201802 -se
```
