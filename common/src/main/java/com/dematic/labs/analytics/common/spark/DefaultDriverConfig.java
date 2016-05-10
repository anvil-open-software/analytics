package com.dematic.labs.analytics.common.spark;

import com.google.common.base.Strings;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDriverConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDriverConfig.class);

    private String appName;
    private String uniqueAppSuffix;
    private String masterUrl;
    private String pollTime;
    private String checkPointDir;
    private StreamConfig streamConfig;

    public String getAppName() {
        return appName;
    }

    public void setAppName(final String appName) {
        this.appName = appName;
    }

    public String getUniqueAppSuffix() {
        return uniqueAppSuffix;
    }

    public void setUniqueAppSuffix(final String uniqueAppSuffix) {
        this.uniqueAppSuffix = uniqueAppSuffix;
    }

    public String getMasterUrl() {
        return masterUrl;
    }

    public void setMasterUrl(final String masterUrl) {
        this.masterUrl = masterUrl;
    }

    public String getPollTime() {
        return pollTime;
    }

    public Duration getPollTimeInSeconds() {
        return Durations.seconds(Integer.valueOf(pollTime));
    }

    public void setPollTime(final String pollTime) {
        this.pollTime = pollTime;
    }

    public String getCheckPointDir() {
        return checkPointDir;
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

    public StreamConfig getStreamConfig() {
        return streamConfig;
    }

    public void setStreamConfig(final StreamConfig streamConfig) {
        this.streamConfig = streamConfig;
    }


}
