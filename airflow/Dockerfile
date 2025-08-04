FROM apache/airflow:3.0.3-python3.11

USER root

ARG FLINK_VERSION=2.0.0
ARG SCALA_VERSION=2.12
# ▶ 1) 가급적 archive 대신 최신 미러 사용
ARG FLINK_URL=https://downloads.apache.org/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz
ARG FLINK_SHA512_URL=${FLINK_URL}.sha512

# 1) 필수 패키지
RUN apt-get update -qq \
 && apt-get install -y --no-install-recommends openjdk-17-jdk ca-certificates curl tini wget \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# 2) Flink CLI + PyFlink SDK
RUN set -eux; \
    # 2‑1) 파일 & 체크섬 다운로드 (재시도 5회, 이어받기)
    curl -fSL --retry 5 --retry-delay 5 --continue-at - "$FLINK_URL" -o /tmp/flink.tgz; \
    curl -fSL "$FLINK_SHA512_URL" -o /tmp/flink.tgz.sha512; \
    sha512sum -c /tmp/flink.tgz.sha512; \
    # 2‑2) 설치
    tar -xzf /tmp/flink.tgz -C /opt; \
    mv /opt/flink-${FLINK_VERSION} /opt/flink; \
    ln -s /opt/flink/bin/flink /usr/local/bin/flink; \
    rm /tmp/flink.tgz*; \
    # 2‑3) PyFlink SDK (Airflow 태스크에서 Python API 사용 가능)
    pip install --no-cache-dir apache-flink==${FLINK_VERSION}

# 3) 권한 및 환경 변수
RUN chown -R airflow:airflow /opt/flink
ENV FLINK_HOME=/opt/flink \
    PATH=$PATH:/opt/flink/bin \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
WORKDIR /opt/airflow