
# Developing against remote yarn cluster

Currently the painful manual steps are:
1. Build jar
 mvn -Dmaven.test.skip=true install -pl ingestion
2. Copy over to master server  i.e.
 scp ingestion/target/ingestion-1.0-SNAPSHOT-driver.jar spark@10.40.217.120:/tmp

3. On the machine, copy into place. We only do this in case you mistakenly overwrite with a Nexus version
 cp /tmp/ingestion-1.0-SNAPSHOT-driver.jar /opt/spark/drivers/

4. Launch Jenkins job for spark submit (make sure "Update Driver" flag is turned off)
