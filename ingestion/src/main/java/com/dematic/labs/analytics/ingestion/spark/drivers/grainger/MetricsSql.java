package com.dematic.labs.analytics.ingestion.spark.drivers.grainger;

public interface MetricsSql {
    String CONFIG_KEYS = "SELECT OPCMetricID, Timestamp FROM (SELECT OPCMetricID, Timestamp, rank() OVER (PARTITION BY " +
            "OPCMetricID ORDER BY Timestamp DESC) as rank FROM config_raw) tmp WHERE tmp.rank = 1";

    String CONFIG = "SELECT config_raw.* FROM config_raw JOIN config_keys ON config_keys.OPCMetricID = " +
            "config_raw.OPCMetricID WHERE config_keys.Timestamp = config_raw.Timestamp and config_keys.OPCMetricID = " +
            "config_raw.OPCMetricID";

    String FIVE_MINUTES = "SELECT readings.*,config.OPCMetricID,config.AggregationType FROM readings JOIN config ON " +
            "readings.OPCTagID = config.OPCTagID WHERE config.AggregationInterval='FiveMinutes'";

    String FIVE_MINUTE_SUM = "SELECT OPCTagID,OPCMetricID, SUM(Value) as Sum, COUNT(Value) as Count, AVG(Value) as " +
            "Average, MIN(Value) as Minimum, MAX(Value) as Maximum from FiveMinutesReadings WHERE AggregationType<>" +
            "'None' GROUP BY OPCTagID, OPCMetricID";

    String FITHTEEN_MINUTES = "SELECT readings.*,config.OPCMetricID,config.AggregationType FROM readings JOIN config " +
            "ON readings.OPCTagID = config.OPCTagID WHERE config.AggregationInterval='FifteenMinutes'";

    String FITHTEEN_MINUTE_SUM = "SELECT OPCTagID,OPCMetricID, SUM(Value) as Sum, COUNT(Value) as Count, AVG(Value) as" +
            " Average, MIN(Value) as Minimum, MAX(Value) as Maximum from FifteenMinutesReadings WHERE AggregationType<>" +
            "'None' GROUP BY OPCTagID, OPCMetricID";

    String THIRTY_MINUTES = "SELECT readings.*,config.OPCMetricID,config.AggregationType FROM readings JOIN config ON " +
            "readings.OPCTagID = config.OPCTagID WHERE config.AggregationInterval='ThirtyMinutes'";

    String THIRTY_MINUTE_SUM = "SELECT OPCTagID,OPCMetricID, SUM(Value) as Sum, COUNT(Value) as Count, AVG(Value) as" +
            " Average, MIN(Value) as Minimum, MAX(Value) as Maximum from ThirtyMinutesReadings WHERE AggregationType<>" +
            "'None' GROUP BY OPCTagID, OPCMetricID";

    String HOUR = "SELECT readings.*,config.OPCMetricID,config.AggregationType FROM readings JOIN config ON " +
            "readings.OPCTagID = config.OPCTagID WHERE config.AggregationInterval='Hour'";

    String HOUR_SUM = "SELECT OPCTagID,OPCMetricID, SUM(Value) as Sum, COUNT(Value) as Count, AVG(Value) as Average, " +
            "MIN(Value) as Minimum, MAX(Value) as Maximum from HourReadings WHERE AggregationType<>'None' GROUP BY " +
            "OPCTagID, OPCMetricID";

    String DAY = "SELECT readings.*,config.OPCMetricID,config.AggregationType FROM readings JOIN config ON " +
            "readings.OPCTagID = config.OPCTagID WHERE config.AggregationInterval='Day'";

    String DAY_SUM = "SELECT OPCTagID,OPCMetricID, SUM(Value) as Sum, COUNT(Value) as Count, AVG(Value) as Average, " +
            "MIN(Value) as Minimum, MAX(Value) as Maximum from DayReadings WHERE AggregationType<>'None' GROUP BY " +
            "OPCTagID, OPCMetricID";


}