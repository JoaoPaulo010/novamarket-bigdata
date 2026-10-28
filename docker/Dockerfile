# Uma única imagem com Airflow + Java + PySpark + cliente Kafka.
# Assim o mesmo container roda o produtor, os jobs Spark e o Airflow.
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.1 kafka-python==2.2.15 pyarrow
