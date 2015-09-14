# start with java 8
FROM dockerfile/java:oracle-java8

MAINTAINER michael.mitchell@dematic.com

ENV REFRESHED_AT 2015-08-14

# update env
RUN apt-get update

# install scala 2.11
RUN wget http://www.scala-lang.org/files/archive/scala-2.11.6.deb
RUN dpkg -i scala-2.11.6.deb
RUN apt-get install scala
RUN rm scala-2.11.6.deb

# install spark 1.4 built with hadoop 2.6, java 8, scala 2.11.6 and Kinesis
RUN curl -s -u ${nexus.Username}:${nexus.Password} http://${nexus.Ip}/nexus/content/repositories/releases/org/apache/spark/1.5.0/spark-1.5.0.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-1.5.0-bin-2.6.0 spark
ENV SPARK_HOME /usr/local/spark

# next steps have docker configure cluster mgr, spark, and start spark master and slaves
#EXPOSE 8080