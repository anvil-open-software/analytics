FROM dematic/spark:1.2.1

MAINTAINER Michael Mitchell "michael.mitchell@dematic.com"

ENV REFRESHED_AT 2015-03-19

# install supervisor
RUN mkdir /var/log/supervisor
RUN yum install -y python-setuptools
RUN easy_install pip
RUN curl https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py -o - | python
RUN pip install supervisor
# add the conf file
ADD supervisord.conf /etc/supervisor/supervisord.conf

# add the uber jar to sparks lib
ADD spark-drivers-1.0-SNAPSHOT.jar /usr/local/spark/lib/spark-drivers-1.0-SNAPSHOT.jar

# deploy drivers, todo: figure out auto deployment of drivers
#CMD ["$SPARK_HOME/bin/spark-submit", "--class", "com.dematic.labs.analytics.ingestion.sparks.drivers.EventCount", "--master", "yarn-client", "--num-executors", "5", "--executor-memory", "1G", "$SPARK_HOME/lib/spark-drivers-1.0-SNAPSHOT.jar", "https://kinesis.us-west-2.amazonaws.com", "eventstream", "AKIAJBZM3CD7FH7GYDQA", "i+bPtKd1tmXyK27UZBxaYn3Nl0M29CqHubbRnqd3"]

COPY supervisord-*.conf /etc/supervisor/conf.d/

# start sparks
CMD ["supervisord", "-n", "-c", "/etc/supervisor/supervisord.conf"]