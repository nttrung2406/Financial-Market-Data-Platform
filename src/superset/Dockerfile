FROM apache/superset:5.0.0

USER root

RUN pip install --no-cache-dir \
    psycopg2-binary \
    sqlalchemy \
    minio

COPY superset/superset_config.py /app/pythonpath/superset_config.py
COPY superset/bootstrap.py       /app/pythonpath/bootstrap.py

ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

USER superset
