FROM python:3.10-slim-bookworm@sha256:5be5aaecb962a41567bc8041f1e9e3432cb085b037fe057f9a562630b2e4b370 AS python
COPY infra/docker/spark-gx.lock /tmp/gx.lock
RUN python -m pip install --no-cache-dir --only-binary=:all: --require-hashes --target=/opt/gx -r /tmp/gx.lock

FROM apache/kafka:4.3.2@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7 AS broker
FROM spark:4.0.5-python3@sha256:95949da1491f7fcec98169c1d4039648aa73330bcd73a9628f7b2e6dc66ffab0 AS spark
FROM postgres:16.16-bookworm@sha256:a49f7438ba03589f9b1ca062f2da82e899b48ea1dc50d2449fa09a70be83ebca
COPY --from=python /usr/local/ /usr/local/
COPY --from=python /opt/gx /opt/gx
COPY --from=spark /opt/java/openjdk /opt/java/openjdk
COPY --from=spark /opt/spark /opt/spark
COPY --from=broker /opt/kafka /opt/kafka
ADD --checksum=sha256:0b2ec2bb5b84dacf4501773aca754bba05c299ca2ae278985f1af260460af955 https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-postgres/3.7.1.Final/debezium-connector-postgres-3.7.1.Final-plugin.tar.gz /tmp/plugin.tgz
RUN mkdir -p /kafka/connect && tar xzf /tmp/plugin.tgz -C /kafka/connect && rm /tmp/plugin.tgz
ADD --chmod=0444 --checksum=sha256:a1e76860b19fe1e6b205a4ea7a9099a2f9575d39bb71d49b3e679b299d0efb91 https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.13/4.0.1/delta-spark_2.13-4.0.1.jar /opt/bronze-jars/delta-spark_2.13-4.0.1.jar
ADD --chmod=0444 --checksum=sha256:ecc42c5aaa51c423312d9e4e48aa4a12b34740bf2c37de0e8f279277ec3d12e4 https://repo.maven.apache.org/maven2/io/delta/delta-storage/4.0.1/delta-storage-4.0.1.jar /opt/bronze-jars/delta-storage-4.0.1.jar
ADD --chmod=0444 --checksum=sha256:33a3b8b04df4a2e43c9d5bf8ca5972936a1ac662f2714e014d00ef6f27468bf3 https://repo.maven.apache.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.0/spark-sql-kafka-0-10_2.13-4.0.0.jar /opt/kafka-jars/spark-sql-kafka-0-10_2.13-4.0.0.jar
ADD --chmod=0444 --checksum=sha256:f2be3b76f9e6cbe63379d4e4e0dc14f59dbf056ae2ad16ff15ac108004c86767 https://repo.maven.apache.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.0/spark-token-provider-kafka-0-10_2.13-4.0.0.jar /opt/kafka-jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar
ADD --chmod=0444 --checksum=sha256:014b4ef36884478a932641818b080c36a8f8e2e92dc771e800c9d2694f4f83d8 https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar /opt/kafka-jars/kafka-clients-3.9.2.jar
ADD --chmod=0444 --checksum=sha256:6d3bd18df8410f3e31b031aca582cc109342358a62a2759ebd0c4cdf30d06f8b https://repo.maven.apache.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar /opt/kafka-jars/commons-pool2-2.12.0.jar
ENV JAVA_HOME=/opt/java/openjdk SPARK_HOME=/opt/spark SPARK_LOCAL_IP=127.0.0.1 PYTHONDONTWRITEBYTECODE=1
ENV PATH=/opt/java/openjdk/bin:/opt/kafka/bin:/opt/spark/bin:$PATH
ENV PYTHONPATH=/app:/opt/gx:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip
RUN chmod 0555 /opt/bronze-jars /opt/kafka-jars && ldconfig && for u in broker connector jobs reader forecast; do useradd -M -s /usr/sbin/nologin $u; done
RUN mkdir -p /home/jovyan && ln -s /var/lib/postgresql/data/forecast /home/jovyan/work && ln -s /var/lib/postgresql/data/reader /stream && ln -s /var/lib/postgresql/data/jobs /gold && ln -s /var/lib/postgresql/data/batch /batch && ln -s /app /workspace && ln -s /app/tests/data/fixtures/m5 /fixtures
WORKDIR /app
COPY . /app
RUN ln -s /app/infra/docker/stokora /usr/local/bin/stokora && chmod 755 /app/infra/docker/stokora
ENTRYPOINT ["python","-u","/app/infra/docker/service.py"]
