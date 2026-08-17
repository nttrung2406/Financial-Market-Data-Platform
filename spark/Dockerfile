FROM apache/spark:3.5.0

USER root

RUN pip install \
    delta-spark==3.2.0 \
    boto3 \
    minio \
    pandas \
    pyarrow \
    psycopg2-binary \
    kafka-python

# Pre-download JARs needed for Delta Lake, MinIO (S3A), Kafka, and PostgreSQL
RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -fSL -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fSL -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fSL -o /opt/bitnami/spark/jars/delta-spark_2.12-3.2.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -fSL -o /opt/bitnami/spark/jars/delta-storage-3.2.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar && \
    curl -fSL -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -fSL -o /opt/bitnami/spark/jars/kafka-clients-3.4.0.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    curl -fSL -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -fSL -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

USER 1001