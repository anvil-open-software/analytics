package com.dematic.labs.analytics.common.spark;

import java.util.Objects;

public class DynamoDbDriverConfig extends DefaultDriverConfig {
    private String dynamoPrefix;
    private String dynamoDBEndpoint;

    public String getDynamoPrefix() {
        return dynamoPrefix;
    }

    public void setDynamoPrefix(final String dynamoPrefix) {
        this.dynamoPrefix = dynamoPrefix;
    }

    public String getDynamoDBEndpoint() {
        return dynamoDBEndpoint;
    }

    public void setDynamoDBEndpoint(final String dynamoDBEndpoint) {
        this.dynamoDBEndpoint = dynamoDBEndpoint;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        DynamoDbDriverConfig that = (DynamoDbDriverConfig) o;
        return Objects.equals(dynamoPrefix, that.dynamoPrefix) &&
                Objects.equals(dynamoDBEndpoint, that.dynamoDBEndpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dynamoPrefix, dynamoDBEndpoint);
    }

    @Override
    public String toString() {
        return "DynamoDbDriverConfig{" +
                "dynamoPrefix='" + dynamoPrefix + '\'' +
                ", dynamoDBEndpoint='" + dynamoDBEndpoint + '\'' +
                "} " + super.toString();
    }
}
