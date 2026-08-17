FROM apache/airflow:3.0.6-python3.11

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    default-jdk \
    procps \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the full src tree so ingestion code is importable as `src.ingestion`
COPY . /opt/airflow/project/src/
RUN touch /opt/airflow/project/src/__init__.py && \
    chown -R airflow: /opt/airflow/project

USER airflow

COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV PYTHONPATH=/opt/airflow/project
