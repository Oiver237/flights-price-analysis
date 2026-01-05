
# Spark container with a writable Ivy cache
FROM apache/spark:3.5.0

USER root

# Logs dir with sane permissions
RUN mkdir -p /opt/spark/logs \
 && chown -R spark:spark /opt/spark/logs \
 && chmod -R 775 /opt/spark/logs

# Install procps to satisfy Spark's use of `ps`
RUN apt-get update \
 && apt-get install -y --no-install-recommends procps \
 && rm -rf /var/lib/apt/lists/*

# Ivy cache location owned by the spark user
ENV SPARK_JARS_IVY=/opt/spark/.ivy2
RUN mkdir -p $SPARK_JARS_IVY && chown -R spark:spark $SPARK_JARS_IVY

# (Optional) make PySpark interpreter explicit
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

USER spark
WORKDIR /opt/spark

# (Optional) healthcheck
# HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
#  CMD bash -c "which ps >/dev/null && /opt/spark/bin/spark-submit --version >/dev/null"
