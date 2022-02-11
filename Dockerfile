FROM python:3.7.4

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    git \
    curl \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install pipenv

WORKDIR /opt/glue

## following this official document
## https://docs.aws.amazon.com/ja_jp/glue/latest/dg/aws-glue-programming-etl-libraries.html

RUN curl -OL https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-common/apache-maven-3.6.0-bin.tar.gz
RUN tar -xzvf apache-maven-3.6.0-bin.tar.gz
RUN curl -OL  https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-2.0/spark-2.4.3-bin-hadoop2.8.tgz
RUN tar -xzvf spark-2.4.3-bin-hadoop2.8.tgz
RUN git clone https://github.com/awslabs/aws-glue-libs.git -b glue-2.0

ENV SPARK_HOME /opt/glue/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8

ENV PATH=$PATH:/opt/glue/aws-glue-libs/bin
RUN chmod -R +x /opt/glue/aws-glue-libs/bin
RUN glue-setup.sh

COPY . ./scripts

WORKDIR /opt/glue/scripts

RUN pipenv install --dev --system --deploy --ignore-pipfile

ENTRYPOINT ["/bin/bash"]
CMD ["-c", "ls"]
