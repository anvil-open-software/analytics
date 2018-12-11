/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.analytics.diagnostics.spark.drivers

object PropertiesUtils {
  def getOrThrow(systemPropertyName: String): String = {
    val property = sys.props(systemPropertyName)
    if (property == null) throw new IllegalStateException(String.format("'%s' needs to be set", systemPropertyName))
    property
  }
}
