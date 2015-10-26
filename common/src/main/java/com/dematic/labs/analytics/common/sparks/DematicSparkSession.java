package com.dematic.labs.analytics.common.sparks;

import com.google.common.base.Strings;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Holder for spark driver session details including dstream
 */
public class DematicSparkSession implements Serializable {


    JavaStreamingContext streamingContext;
    JavaDStream<byte[]> dStreams;

    String streamName;
    String awsEndPoint;

    String checkPointDir;
    String appName;

    private static final long serialVersionUID = 1896518324147474596L;
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

    public JavaDStream<byte[]> getDStreams() {
        return dStreams;
    }

    public void setDStreams(JavaDStream<byte[]> dStreams) {
        LOGGER.info("Setting DStreams + " + dStreams);
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
        this.checkPointDir = System.getProperty("spark.checkpoint.dir");
        if (Strings.isNullOrEmpty(checkPointDir) && failIfNotSet) {
            throw new IllegalArgumentException("'spark.checkpoint.dir' jvm parameter needs to be set");
        }
        LOGGER.info("using >{}< checkpoint dir", checkPointDir);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DematicSparkSession that = (DematicSparkSession) o;

        if (streamingContext != null ? !streamingContext.equals(that.streamingContext) : that.streamingContext != null)
            return false;
        if (dStreams != null ? !dStreams.equals(that.dStreams) : that.dStreams != null) return false;
        if (streamName != null ? !streamName.equals(that.streamName) : that.streamName != null) return false;
        if (awsEndPoint != null ? !awsEndPoint.equals(that.awsEndPoint) : that.awsEndPoint != null) return false;
        if (checkPointDir != null ? !checkPointDir.equals(that.checkPointDir) : that.checkPointDir != null)
            return false;
        return !(appName != null ? !appName.equals(that.appName) : that.appName != null);

    }

    @Override
    public int hashCode() {
        int result = streamingContext != null ? streamingContext.hashCode() : 0;
        result = 31 * result + (dStreams != null ? dStreams.hashCode() : 0);
        result = 31 * result + (streamName != null ? streamName.hashCode() : 0);
        result = 31 * result + (awsEndPoint != null ? awsEndPoint.hashCode() : 0);
        result = 31 * result + (checkPointDir != null ? checkPointDir.hashCode() : 0);
        result = 31 * result + (appName != null ? appName.hashCode() : 0);
        return result;
    }
}
