package com.dematic.labs.analytics.store.sparks.drivers.cassandra;

import com.dematic.labs.analytics.common.spark.DriverConfig;
import com.google.common.base.Strings;

final class PersisterDriverConfig extends DriverConfig {
    private String keySpace;
    private String host;
    private final String username;
    private final String password;

    PersisterDriverConfig() {
        username = System.getProperty("spark.cassandra.auth.username");
        password = System.getProperty("spark.cassandra.auth.password");
        if (Strings.isNullOrEmpty(username)) {
            throw new IllegalStateException(">spark.cassandra.auth.username< can't be 'null'");
        }
        if (Strings.isNullOrEmpty(password)) {
            throw new IllegalStateException(">spark.cassandra.auth.password< can't be 'null'");
        }
    }

    String getKeySpace() {
        return keySpace;
    }

    void setKeySpace(final String keySpace) {
        this.keySpace = keySpace;
    }

    String getHost() {
        return host;
    }

    void setHost(final String host) {
        this.host = host;
    }

    String getUsername() {
        return username;
    }

    String getPassword() {
        return password;
    }

    @Override
    public String toString() {
        return "PersisterDriverConfig{" +
                "keySpace='" + keySpace + '\'' +
                ", host='" + host + '\'' +
                ", username='" + username + '\'' +
                "} " + super.toString();
    }
}
