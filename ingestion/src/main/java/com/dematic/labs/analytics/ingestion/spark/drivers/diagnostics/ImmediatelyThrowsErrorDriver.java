/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */
package com.dematic.labs.analytics.ingestion.spark.drivers.diagnostics;


/**
 *
 * Driver which immediately throws an error to test error handling machinery
 */
public final class ImmediatelyThrowsErrorDriver {
    public static void main(String[] args) {
       throw new NullPointerException("ImmediatelyThrowsErrorDriver throws NullPointerException on Launch");
    }
}