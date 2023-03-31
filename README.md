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
