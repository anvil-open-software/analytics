/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.analytics.common.spark;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public interface StreamConfig extends Serializable {
    String getStreamEndpoint();
    void setStreamEndpoint(final String streamEndpoint);

    String getStreamName();
    void setStreamName(final String streamName);

    String getGroupId();
    void setGroupId(final String groupId);

    Map<String, Object> getAdditionalConfiguration();
    void setAdditionalConfiguration(final Map<String, Object> additionalConfiguration);

    Set<String> getTopics();
}
