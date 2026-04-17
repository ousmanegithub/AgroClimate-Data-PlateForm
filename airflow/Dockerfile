FROM apache/airflow:2.10.4-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless wget && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN mkdir -p /opt/spark/jars && \
    wget -O /opt/spark/jars/postgresql-42.7.4.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.2 psycopg2-binary==2.9.9 apache-airflow-providers-postgres==5.13.1
