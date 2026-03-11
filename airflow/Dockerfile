FROM apache/airflow:2.8.1

USER root

# 1. Install Java (Prerequisite for Spark execution)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# 2. Install Python dependencies
# pyspark: Core library for data processing
# apache-airflow-providers-google: Required for GCS/BigQuery integration
RUN pip install --no-cache-dir \
    pyspark \
    apache-airflow-providers-google \
    pandas \
    requests \
    dbt-bigquery