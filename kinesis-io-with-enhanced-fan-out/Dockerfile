FROM flink:1.15.2-scala_2.12-java11

# Install tools for simulating various networking conditions
RUN apt-get update && \
    apt-get install -y iproute2 iputils-ping

ADD docker/log4j.properties /opt/flink/conf/log4j.properties
ADD docker/log4j.properties /opt/flink/conf/log4j-session.properties
ADD docker/log4j.properties /opt/flink/conf/log4j-console.properties
