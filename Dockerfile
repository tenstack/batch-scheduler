FROM centos:centos7
LABEL description="batch scheduler"

COPY ./bin/batch-scheduler /scheduler
ENTRYPOINT ["/scheduler"]