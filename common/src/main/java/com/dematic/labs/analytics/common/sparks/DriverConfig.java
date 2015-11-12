package com.dematic.labs.analytics.common.sparks;

import com.google.common.base.Strings;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Holder for spark driver session details including input parameters
 */
public class DriverConfig implements Serializable {


    private String appName;
    private String uniqueAppSuffix;

    private String streamName;
    private String kinesisEndpoint;

    private String dynamoDBEndpoint;
    private String dynamoPrefix;

    private Duration pollTime;
    private TimeUnit timeUnit;

    private String checkPointDir;
    private static final long serialVersionUID = 1896518324147474596L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DriverConfig.class);

    public DriverConfig(String uniqueAppSuffix, String args[]) {
        // used to formulate app name
        this.uniqueAppSuffix=uniqueAppSuffix;
        setParametersFromArguments(args);
    }

    //todo: fix
    public void setParametersFromArguments(String args[]) {
        if (args.length < 5) {
            throw new IllegalArgumentException("Driver requires Kinesis Endpoint, Kinesis StreamName, DynamoDB Endpoint,"
                    + "optional DynamoDB Prefix, driver PollTime, and aggregation by time {MINUTES,DAYS}");
        }
        this.kinesisEndpoint = args[0];
        this.streamName = args[1];
        this.dynamoDBEndpoint = args[2];

        if (args.length == 5) {
            dynamoPrefix = null;
            pollTime = Durations.seconds(Integer.valueOf(args[3]));
            timeUnit = TimeUnit.valueOf(args[4]);
        } else {
            dynamoPrefix = args[3];
            pollTime = Durations.seconds(Integer.valueOf(args[4]));
            timeUnit = TimeUnit.valueOf(args[5]);
        }
        appName = Strings.isNullOrEmpty(dynamoPrefix) ? uniqueAppSuffix :
                      String.format("%s%s", dynamoPrefix, uniqueAppSuffix);

    }

    public String getKinesisEndpoint() {
        return kinesisEndpoint;
    }

    public String getDynamoDBEndpoint() {
        return dynamoDBEndpoint;
    }

    public String getDynamoPrefix() {
        return dynamoPrefix;
    }

    public Duration getPollTime() {
        return pollTime;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public String getAppName() {
        return appName;
    }


    public String getStreamName() {
        return streamName;
    }

    public String getCheckPointDir() {
        return checkPointDir;
    }

    /**
     *
     * @param failIfNotSet if true, will throw exception if property does not exist
     */
    public void setCheckPointDirectoryFromSystemProperties(boolean failIfNotSet) {
        this.checkPointDir = System.getProperty(DriverConsts.SPARK_CHECKPOINT_DIR);
        if (Strings.isNullOrEmpty(checkPointDir) && failIfNotSet) {
            throw new IllegalArgumentException(DriverConsts.SPARK_CHECKPOINT_DIR + " jvm parameter needs to be set");
        }
        LOGGER.info("using >{}< checkpoint dir", checkPointDir);
    }


}
