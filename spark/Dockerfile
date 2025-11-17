FROM apache/spark:3.5.0

USER root
RUN mkdir -p /opt/spark/logs && chmod 777 /opt/spark/logs
# RUN pip install apache-airflow apache-airflow-providers-apache-spark
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf