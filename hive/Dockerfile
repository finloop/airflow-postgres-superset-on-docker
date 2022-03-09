FROM finloop/hadoop-base:0.1

ENV HIVE_VERSION 3.1.2
ENV HIVE_URL https://dlcdn.apache.org/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz
ENV HIVE_HOME /opt/apache-hive-${HIVE_VERSION}-bin

RUN curl -fSL "$HIVE_URL" -o /tmp/hive.tar.gz \
    && tar -xvf /tmp/hive.tar.gz -C /opt \
    && rm /tmp/hive.tar.gz

RUN curl -fSL "https://jdbc.postgresql.org/download/postgresql-42.3.3.jar" -o $HIVE_HOME/lib/postgresql-jdbc.jar

RUN apt-get update && apt-get install -y --no-install-recommends procps

COPY startup.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/startup.sh

COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 10000
EXPOSE 10002
EXPOSE 9999

ENTRYPOINT ["entrypoint.sh"]
CMD startup.sh
