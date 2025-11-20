FROM apache/airflow:2.8.1

COPY requirements.txt /opt/airflow/requirements.txt

USER root
RUN apt-get update && apt-get install -y gcc python3-dev
RUN mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins
RUN chown -R airflow /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    wget \
    curl \
    net-tools iputils-ping \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && chown -R airflow:root /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Remove Docker socket group configuration since we don't need it
# RUN DOCKER_GID=$(stat -c '%g' /var/run/docker.sock 2>/dev/null || echo 999) && \
#     groupadd -g $DOCKER_GID docker 2>/dev/null || true && \
#     usermod -aG $DOCKER_GID airflow

USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

ENTRYPOINT ["/entrypoint"]
CMD ["bash"]