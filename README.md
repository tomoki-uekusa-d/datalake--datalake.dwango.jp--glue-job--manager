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
