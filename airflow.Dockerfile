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

USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

ENTRYPOINT ["/entrypoint"]
CMD ["bash"]