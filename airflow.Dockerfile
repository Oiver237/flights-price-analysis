
# Airflow image with Spark 3.5.0 and build tools
FROM apache/airflow:2.8.1

# Copy requirements early to leverage build cache
COPY requirements.txt /opt/airflow/requirements.txt

# -------------------------------
# System packages & Java (Spark)
# -------------------------------
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        wget \
        ca-certificates \
        build-essential \
        python3-dev \
        libssl-dev \
        libffi-dev \
        pkg-config \
        procps && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Upgrade Python build tooling for better wheel resolution
RUN python -m pip install --upgrade pip setuptools wheel

# -------------------------------
# Install Spark 3.5.0
# -------------------------------
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && chown -R airflow:root /opt/spark

# Spark env (DO NOT change HOME; keep default /home/airflow)
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
# py4j version bundled with Spark 3.5.0 examples
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Robust Ivy cache (explicit path; independent of HOME)
ENV SPARK_JARS_IVY=/home/airflow/.ivy2
RUN mkdir -p $SPARK_JARS_IVY && chown -R airflow:root $SPARK_JARS_IVY

# Default Spark config (repos + Ivy path + S3A impl)
# NOTE: we REMOVE spark.jars.packages to avoid runtime Ivy resolution
RUN mkdir -p /opt/spark/conf && \
    printf "%s\n" \
      "spark.jars.repositories https://repo1.maven.org/maven2" \
      "spark.jars.ivy /home/airflow/.ivy2" \
      "spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem" \
    > /opt/spark/conf/spark-defaults.conf && \
    chown -R airflow:root /opt/spark/conf

# --------------------------------
# Vendor S3A jars (offline-friendly)
# --------------------------------
# Use versions compatible with Hadoop 3.3.x in Spark 3.5
# hadoop-aws provides the S3A connector; aws-java-sdk-bundle provides AWS SDK
RUN wget -q -P /opt/spark/jars \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q -P /opt/spark/jars \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# -------------------------------
# Airflow directories
# -------------------------------
RUN mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins && \
    chown -R airflow:root /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins

# --------------------------------
# Install Python dependencies
# --------------------------------
USER airflow
RUN pip install --no-cache-dir --user -r /opt/airflow/requirements.txt

# Healthcheck to catch regressions early
# HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
#   CMD bash -lc "which ps >/dev/null && spark-submit --version >/dev/null && ls /opt/spark/jars | grep -E 'hadoop-aws|aws-java-sdk-bundle' >/dev/null"

# Runtime as 'airflow'
ENTRYPOINT ["/entrypoint"]
CMD ["bash"]
