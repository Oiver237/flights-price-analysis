FROM apache/airflow:2.8.1

COPY requirements.txt /opt/airflow/requirements.txt

USER root
RUN apt-get update && apt-get install -y gcc python3-dev
RUN mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins
RUN chown -R airflow /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins

USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

ENTRYPOINT ["/entrypoint"]
CMD ["bash"]