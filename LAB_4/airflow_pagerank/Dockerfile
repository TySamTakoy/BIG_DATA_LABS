FROM apache/airflow:2.9.0-python3.10

USER root

# Установка Java и Spark
RUN apt-get update && \
    apt-get install -y curl openjdk-17-jdk && \
    curl -L https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
    | tar -xz -C /opt && \
    ln -s /opt/spark-3.5.0-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

USER airflow
