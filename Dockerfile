FROM apache/airflow:2.9.1
USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
RUN export JAVA_HOME

USER airflow

COPY ./requirements.txt /
RUN pip install -r /requirements.txt

COPY --chown=airflow:root ./dags /opt/airflow/dags