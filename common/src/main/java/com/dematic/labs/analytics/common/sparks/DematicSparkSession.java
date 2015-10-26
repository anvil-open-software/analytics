package com.dematic.labs.analytics.common.sparks;

import com.google.common.base.Strings;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DematicSparkSession {

    JavaStreamingContext streamingContext;
    JavaDStream<byte[]> dStreams;

    String streamName;
    String awsEndPoint;

    String checkPointDir;
    String appName;
    private static final Logger LOGGER = LoggerFactory.getLogger(DematicSparkSession.class);

    public DematicSparkSession(String appName, String awsEndPoint, String streamName) {
        this.appName=appName;
        this.streamName = streamName;
        this.awsEndPoint = awsEndPoint;
    }

    public String getAppName() {
        return appName;
    }

    public JavaStreamingContext getStreamingContext() {
        return streamingContext;
    }

    public void setStreamingContext(JavaStreamingContext streamingContext) {
        this.streamingContext = streamingContext;
    }

    public JavaDStream<byte[]> getdStreams() {
        return dStreams;
    }

    public void setDStreams(JavaDStream<byte[]> dStreams) {
        this.dStreams = dStreams;
    }

    public String getAwsEndPoint() {
        return awsEndPoint;
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
        String checkPointDir = System.getProperty("spark.checkpoint.dir");
        if (Strings.isNullOrEmpty(checkPointDir) && failIfNotSet) {
            throw new IllegalArgumentException("'spark.checkpoint.dir' jvm parameter needs to be set");
        }
        LOGGER.info("using >{}< checkpoint dir", checkPointDir);

    }

}
