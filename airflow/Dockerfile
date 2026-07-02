FROM apache/airflow:2.10.2

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install \
    pyspark \
    delta-spark \
    yfinance \
    pandas==2.2.3