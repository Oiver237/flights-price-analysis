
# Airflow image (sans Spark)
FROM apache/airflow:2.8.1
COPY requirements.txt /opt/airflow/requirements.txt

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
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

RUN python -m pip install --upgrade pip setuptools wheel


RUN mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins && \
    chown -R airflow:root /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins

USER airflow
RUN pip install --no-cache-dir --user -r /opt/airflow/requirements.txt

ENTRYPOINT ["/entrypoint"]
CMD ["bash"]
