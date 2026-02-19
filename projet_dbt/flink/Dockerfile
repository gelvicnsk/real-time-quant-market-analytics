FROM apache/flink:1.18.1-scala_2.12-java11

USER root

# ======================
# Install Python (PyFlink needs python executable)
# ======================
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-venv && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ======================
# Flink SQL connectors (MUST be in /opt/flink/lib for Table/SQL discovery)
# ======================
COPY jars/flink-sql-connector-kafka-3.0.1-1.18.jar /opt/flink/lib/
COPY jars/flink-connector-jdbc-3.1.2-1.18.jar /opt/flink/lib/
COPY jars/postgresql-42.7.3.jar /opt/flink/lib/

# ======================
# PyFlink jobs
# ======================
COPY jobs /jobs

RUN chown -R flink:flink /opt/flink/lib /jobs

USER flink
