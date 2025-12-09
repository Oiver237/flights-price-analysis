
# Spark container with a writable Ivy cache (optional but harmless)
FROM apache/spark:3.5.0

USER root
RUN mkdir -p /opt/spark/logs && chmod 777 /opt/spark/logs

# (Optional) If you keep a spark-defaults.conf in your repo, you can still copy it
# COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Provide an Ivy cache for the 'spark' user (not strictly required—driver resolves packages)
ENV SPARK_JARS_IVY=/opt/spark/.ivy2
RUN mkdir -p $SPARK_JARS_IVY && chown -R spark:spark $SPARK_JARS_IVY

USER spark
WORKDIR /opt/spark
