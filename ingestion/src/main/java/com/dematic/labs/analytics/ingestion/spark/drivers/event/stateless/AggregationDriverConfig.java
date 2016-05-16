package com.dematic.labs.analytics.ingestion.spark.drivers.event.stateless;

import com.dematic.labs.analytics.common.spark.DynamoDbDriverConfig;
import com.dematic.labs.analytics.common.spark.KinesisStreamConfig;
import com.dematic.labs.analytics.common.spark.StreamConfig;
import com.google.common.base.Strings;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
public final class AggregationDriverConfig extends DynamoDbDriverConfig {
    private TimeUnit timeUnit;

    public AggregationDriverConfig() {
    }

    public AggregationDriverConfig(final String uniqueAppSuffix, final String args[]) {
        // used to formulate app name
        setUniqueAppSuffix(uniqueAppSuffix);
        setParametersFromArgumentsForAggregation(args);
    }

    private void setParametersFromArgumentsForAggregation(final String args[]) {
        if (args.length < 5) {
            throw new IllegalArgumentException("Driver requires Kinesis Endpoint, Kinesis StreamName, DynamoDB Endpoint,"
                    + "optional DynamoDB Prefix, driver PollTime, and aggregation by time {MINUTES,DAYS}");
        }
        final StreamConfig streamConfig = new KinesisStreamConfig();
        streamConfig.setStreamEndpoint(args[0]);
        streamConfig.setStreamName(args[1]);
        // set kinesis config
        setStreamConfig(streamConfig);
        // set dynamo config
        setDynamoDBEndpoint(args[2]);

        if (args.length == 5) {
            setDynamoPrefix(null);
            setPollTime(args[3]);
            timeUnit = TimeUnit.valueOf(args[4]);
        } else {
            setDynamoPrefix(args[3]);
            setPollTime(args[4]);
            timeUnit = TimeUnit.valueOf(args[5]);
        }
        setAppName(Strings.isNullOrEmpty(getDynamoPrefix()) ? getUniqueAppSuffix() :
                String.format("%s%s", getDynamoPrefix(), getUniqueAppSuffix()));
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(final TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AggregationDriverConfig that = (AggregationDriverConfig) o;
        return timeUnit == that.timeUnit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), timeUnit);
    }

    @Override
    public String toString() {
        return "AggregationDriverConfig{" +
                "timeUnit=" + timeUnit +
                "} " + super.toString();
    }
}
