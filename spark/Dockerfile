FROM spark:3.5.7-java17-python3
USER root
COPY scripts/download_jars.sh /opt/bootstrap/download_jars.sh
COPY scripts/jars.sha256 /opt/bootstrap/jars.sha256
RUN bash /opt/bootstrap/download_jars.sh /opt/spark/jars
COPY spark/conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY spark/start-thrift.sh /opt/bootstrap/start-thrift.sh
RUN mkdir -p /opt/spark/work /opt/spark/warehouse /opt/spark/logs /opt/spark/events \
    && chown -R 185:0 /opt/spark/work /opt/spark/warehouse /opt/spark/logs /opt/spark/events
USER 185
WORKDIR /opt/spark/work
ENTRYPOINT ["/bin/bash", "/opt/bootstrap/start-thrift.sh"]
