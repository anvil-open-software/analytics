package com.dematic.labs.analytics.common.spark;

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
// todo: still needs more cleanup
public class DriverConfig implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DriverConfig.class);
    private static final long serialVersionUID = 1896518324147474596L;

    private String appName;
    private String uniqueAppSuffix;

    private String kinesisStreamName;
    private String kinesisEndpoint;

    private String dynamoDBEndpoint;
    private String dynamoPrefix;

    private String masterUrl;
    private Duration pollTime;
    private TimeUnit timeUnit;
    // todo: add in another class
    private String mediumInterArrivalTime;
    private String bufferTime;

    private String checkPointDir;

    public DriverConfig() {
    }

    public DriverConfig(final String uniqueAppSuffix, final String args[]) {
        // used to formulate app name
        this.uniqueAppSuffix = uniqueAppSuffix;
        setParametersFromArgumentsForAggregation(args);
    }

    public void setParametersFromArgumentsForAggregation(final String args[]) {
        if (args.length < 5) {
            throw new IllegalArgumentException("Driver requires Kinesis Endpoint, Kinesis StreamName, DynamoDB Endpoint,"
                    + "optional DynamoDB Prefix, driver PollTime, and aggregation by time {MINUTES,DAYS}");
        }
        this.kinesisEndpoint = args[0];
        this.kinesisStreamName = args[1];
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

    public String getAppName() {
        return appName;
    }

    public String getKinesisStreamName() {
        return kinesisStreamName;
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

    public String getMasterUrl() {
        return masterUrl;
    }

    public Duration getPollTime() {
        return pollTime;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public String getMediumInterArrivalTime() {
        return mediumInterArrivalTime;
    }

    public String getBufferTime() {
        return bufferTime;
    }

    public String getCheckPointDir() {
        return checkPointDir;
    }

    public void setAppName(final String appName) {
        this.appName = appName;
    }

    public void setKinesisStreamName(final String kinesisStreamName) {
        this.kinesisStreamName = kinesisStreamName;
    }

    public void setKinesisEndpoint(final String kinesisEndpoint) {
        this.kinesisEndpoint = kinesisEndpoint;
    }

    public void setDynamoDBEndpoint(final String dynamoDBEndpoint) {
        this.dynamoDBEndpoint = dynamoDBEndpoint;
    }

    public void setDynamoPrefix(final String dynamoPrefix) {
        this.dynamoPrefix = dynamoPrefix;
    }

    public void setMasterUrl(final String masterUrl) {
        this.masterUrl = masterUrl;
    }

    public void setPollTime(final String pollTime) {
        this.pollTime = Durations.seconds(Integer.valueOf(pollTime));
    }

    public void setTimeUnit(final TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public void setMediumInterArrivalTime(final String mediumInterArrivalTime) {
        this.mediumInterArrivalTime = mediumInterArrivalTime;
    }

    public void setBufferTime(String bufferTime) {
        this.bufferTime = bufferTime;
    }

    public void setCheckPointDir(final String checkPointDir) {
        this.checkPointDir = checkPointDir;
    }

    /**
     * @param failIfNotSet if true, will throw exception if property does not exist
     */
    public void setCheckPointDirectoryFromSystemProperties(final boolean failIfNotSet) {
        this.checkPointDir = System.getProperty(DriverConsts.SPARK_CHECKPOINT_DIR);
        if (Strings.isNullOrEmpty(checkPointDir) && failIfNotSet) {
            throw new IllegalArgumentException(DriverConsts.SPARK_CHECKPOINT_DIR + " jvm parameter needs to be set");
        }
        LOGGER.info("using >{}< checkpoint dir", checkPointDir);
    }

    @Override
    public String toString() {
        return "DriverConfig{" +
                "appName='" + appName + '\'' +
                ", uniqueAppSuffix='" + uniqueAppSuffix + '\'' +
                ", kinesisStreamName='" + kinesisStreamName + '\'' +
                ", kinesisEndpoint='" + kinesisEndpoint + '\'' +
                ", dynamoDBEndpoint='" + dynamoDBEndpoint + '\'' +
                ", dynamoPrefix='" + dynamoPrefix + '\'' +
                ", masterUrl='" + masterUrl + '\'' +
                ", pollTime=" + pollTime +
                ", timeUnit=" + timeUnit +
                ", mediumInterArrivalTime='" + mediumInterArrivalTime + '\'' +
                ", bufferTime='" + bufferTime + '\'' +
                ", checkPointDir='" + checkPointDir + '\'' +
                '}';
    }
}
