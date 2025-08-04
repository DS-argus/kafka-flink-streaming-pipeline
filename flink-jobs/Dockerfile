FROM flink:2.0.0

# Install JDK, Python3, and pip3
RUN apt-get update -y && \
    apt-get install -y openjdk-11-jdk python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Remove existing /opt/java/openjdk and link installed JDK
RUN rm -rf /opt/java/openjdk && \
    ln -sT /usr/lib/jvm/java-11-openjdk-* /opt/java/openjdk

RUN ln -s /usr/bin/python3 /usr/bin/python

# Install PyFlink
RUN pip3 install apache-flink==2.0.0

    #--- Kafka SQL 커넥터(fat JAR) ---
RUN wget -O /opt/flink/lib/flink-sql-connector-kafka-4.0.0-2.0.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar && \
    # --- JDBC core 커넥터 ---
    wget -O /opt/flink/lib/flink-connector-jdbc-4.0.0-2.0.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-core/4.0.0-2.0/flink-connector-jdbc-core-4.0.0-2.0.jar && \
    # --- JDBC postgres 커넥터 ---
    wget -O /opt/flink/lib/flink-connector-jdbc-postgres-4.0.0-2.0.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-postgres/4.0.0-2.0/flink-connector-jdbc-postgres-4.0.0-2.0.jar && \
    # --- PostgreSQL 드라이버 ---
    wget -O /opt/flink/lib/postgresql-42.7.3.jar \
        https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
