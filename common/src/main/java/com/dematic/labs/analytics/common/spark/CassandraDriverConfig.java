package com.dematic.labs.analytics.common.spark;

import com.google.common.base.Strings;

public final class CassandraDriverConfig extends DriverConfig {
    static final String AUTH_USERNAME_PROP = "spark.cassandra.auth.username";
    static final String AUTH_PASSWORD_PROP = "spark.cassandra.auth.password";
    static final String CONNECTION_HOST_PROP = "spark.cassandra.connection.host";

    private String keySpace;
    private String host;
    private final String username;
    private final String password;

    public CassandraDriverConfig() {
        username = System.getProperty(AUTH_USERNAME_PROP);
        password = System.getProperty(AUTH_PASSWORD_PROP);
        if (Strings.isNullOrEmpty(username)) {
            throw new IllegalStateException(String.format(">%s< can't be 'null'", AUTH_USERNAME_PROP));
        }
        if (Strings.isNullOrEmpty(password)) {
            throw new IllegalStateException(String.format(">%s< can't be 'null'", AUTH_PASSWORD_PROP));
        }
    }

    public String getKeySpace() {
        return keySpace;
    }

    public void setKeySpace(final String keySpace) {
        this.keySpace = keySpace;
    }

    String getHost() {
        return host;
    }

    public void setHost(final String host) {
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
