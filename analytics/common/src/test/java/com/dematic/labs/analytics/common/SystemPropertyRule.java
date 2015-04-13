package com.dematic.labs.analytics.common;

import org.junit.rules.ExternalResource;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SystemPropertyRule extends ExternalResource {
    // load system properties from the junit.properties file
    @Override
    protected void before() throws Throwable {
        final String junitPropertiesPath = String.format("%s/.m2/junit.properties", System.getProperty("user.home"));
        final Stream<String> propertiesStream = Files.lines(Paths.get(junitPropertiesPath));

        final List<String> properties = propertiesStream.collect(Collectors.toList());
        for (final String property : properties) {
            final String[] split = property.split("=");
            final String propertyKey = split[0];
            if (!propertyKey.isEmpty() && !propertyKey.startsWith("#") && !propertyKey.startsWith("nexus")) {
                final String propertyValue = split[1];
                // special case for kinesisInputStream
                if (propertyKey.equals("kinesisInputStream")) {
                    System.setProperty(propertyKey, String.format("%s_stream", System.getProperty("user.name")));
                } else {
                    // add to system properties
                    System.setProperty(propertyKey, propertyValue);
                }
            }
        }
    }
}
