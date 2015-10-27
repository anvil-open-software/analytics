package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.dematic.labs.analytics.common.sparks.DematicSparkSession;

import java.io.Serializable;

/**
 * Generic interface so the same launch utility can be used
 */

public interface EventStreamProcessor extends Serializable {

     void processEvents(DematicSparkSession inSession);
}
