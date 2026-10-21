FROM apache/spark:3.5.2
USER root
# JDK 11 ships with the image; add Maven + Python tooling for the offline
# fallback data generator. HADOOP_CONF_DIR is supplied at runtime via the
# ./hadoop-conf volume mount, so hdfs://mycluster resolves natively.
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-11-jdk-headless \
      maven \
      curl \
      python3 \
      python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && /usr/lib/jvm/java-11-openjdk-amd64/bin/javac -version \
    && mvn -version \
    && java -version
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    HDFS_URI=hdfs://mycluster \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
    SPARK_MASTER="local[*]" \
    PATH="/opt/spark/bin:${PATH}"
WORKDIR /workspace
CMD ["sleep", "infinity"]
