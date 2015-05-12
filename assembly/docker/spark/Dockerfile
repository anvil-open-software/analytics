FROM sequenceiq/hadoop-docker:2.6.0

MAINTAINER michael.mitchell@dematic.com

ENV REFRESHED_AT 2015-04-05

# Install java 8 and make default
RUN mkdir -p /usr/java/default && \
    curl -Ls 'http://download.oracle.com/otn-pub/java/jdk/8u31-b13/jdk-8u31-linux-x64.tar.gz' -H 'Cookie: oraclelicense=accept-securebackup-cookie' | \
    tar --strip-components=1 -xz -C /usr/java/default/

# Update the image with the latest packages (recommended)
RUN yum update -y; yum clean all

# copy yarn env script
COPY yarn-env.sh /usr/local/hadoop/etc/hadoop/yarn-env.sh

# pull spark's built with kinesis lib from nexus and install, spark includes version 1.3.1 and hadoop 2.6, built with scala 2.11
RUN curl -s -u ${nexus.Username}:${nexus.Password} http://${nexus.Ip}/nexus/content/repositories/releases/org/apache/spark/1.3.1/spark-1.3.1.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-1.3.1-bin-2.6.0 spark
ENV SPARK_HOME /usr/local/spark
RUN mkdir $SPARK_HOME/yarn-remote-client
ADD yarn-remote-client $SPARK_HOME/yarn-remote-client

RUN $BOOTSTRAP && $HADOOP_PREFIX/bin/hadoop dfsadmin -safemode leave && $HADOOP_PREFIX/bin/hdfs dfs -put $SPARK_HOME-1.3.1-bin-2.6.0/lib /spark

ENV YARN_CONF_DIR $HADOOP_PREFIX/etc/hadoop
ENV SPARK_JAR hdfs:///spark/spark-assembly-1.3.1-hadoop2.6.0.jar
ENV PATH $PATH:$SPARK_HOME/bin:$HADOOP_PREFIX/bin

# update boot script
COPY bootstrap.sh /etc/bootstrap.sh
RUN chown root.root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

ENTRYPOINT ["/etc/bootstrap.sh"]