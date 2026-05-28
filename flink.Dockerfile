FROM flink:1.18.1-scala_2.12-java17

# Install Python and pip
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev build-essential && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink and Kafka connector
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN pip3 install --no-cache-dir --default-timeout=100 --upgrade pip setuptools wheel
RUN pip3 install --no-cache-dir --default-timeout=100 pytz apache-beam==2.48.0
RUN pip3 install --no-cache-dir --default-timeout=100 apache-flink==1.18.1

# Download Kafka connector jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

WORKDIR /opt/flink/usrlib
COPY flink_jobs/ ./flink_jobs/

# Default command can be overridden to run specific jobs
