FROM dematic/spark:1.3.1

MAINTAINER Michael Mitchell "michael.mitchell@dematic.com"

ENV REFRESHED_AT 2015-04-05

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

COPY supervisord-*.conf /etc/supervisor/conf.d/

# start sparks
CMD ["supervisord", "-n", "-c", "/etc/supervisor/supervisord.conf"]