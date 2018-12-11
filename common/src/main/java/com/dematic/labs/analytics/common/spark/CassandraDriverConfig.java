/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.analytics.common.spark;

import com.google.common.base.Strings;

import java.util.Objects;

@SuppressWarnings("WeakerAccess")
public class CassandraDriverConfig extends DefaultDriverConfig {
    public static final String AUTH_USERNAME_PROP = "spark.cassandra.auth.username";
    public static final String AUTH_PASSWORD_PROP = "spark.cassandra.auth.password";
    public static final String CONNECTION_HOST_PROP = "spark.cassandra.connection.host";
    public static final String KEEP_ALIVE_PROP = "spark.cassandra.connection.keep_alive_ms";

    private String keySpace;
    private String host;
    private String keepAlive;
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

    public String getHost() {
        return host;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    public String getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(final String keepAlive) {
        this.keepAlive = keepAlive;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        CassandraDriverConfig that = (CassandraDriverConfig) o;
        return Objects.equals(keySpace, that.keySpace) &&
                Objects.equals(host, that.host) &&
                Objects.equals(keepAlive, that.keepAlive) &&
                Objects.equals(username, that.username) &&
                Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keySpace, host, keepAlive, username, password);
    }

    @Override
    public String toString() {
        return "CassandraDriverConfig{" +
                "keySpace='" + keySpace + '\'' +
                ", host='" + host + '\'' +
                ", keepAlive='" + keepAlive + '\'' +
                ", username='" + username + '\'' +
                "} " + super.toString();
    }
}
