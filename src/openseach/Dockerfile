FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python \
    opensearch-py \
    requests

COPY kafka_to_opensearch.py ./
COPY setup_dashboards.py    ./
COPY index_templates/       ./index_templates/

CMD ["python", "kafka_to_opensearch.py"]
