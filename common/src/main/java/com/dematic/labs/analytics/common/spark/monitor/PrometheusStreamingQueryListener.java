package com.dematic.labs.analytics.common.spark.monitor;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Uses prometheus java client https://github.com/prometheus/client_java
 * to register structured streaming query stats to a push gateway.
 */

public class PrometheusStreamingQueryListener extends StreamingQueryListener {

    public static final String SPARK_METRIC_PREFIX = "spark_structured_streaming_";
    public static final String LABEL_NAME = "driver";
    // might have to parameterize this
    public static final String job_name = "spark-push-gateway";
    private String push_gateway_host;


    private String app_name;
    CollectorRegistry collectorRegistry;

    // collectors for spark streaming interactive query stats
    private Counter total_batches;
    private Counter total_input_rows;
    private Gauge processedRowsPerSecond;
    private Gauge inputRowsPerSecond;

    private boolean addSparkQueryStats = true;

    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusStreamingQueryListener.class);

    public PrometheusStreamingQueryListener(String spark_app_name, String push_gateway_host) {
        LOGGER.info("Push gateway statistics posted to: " + push_gateway_host);
        this.push_gateway_host = push_gateway_host;
        app_name = spark_app_name;
        collectorRegistry = CollectorRegistry.defaultRegistry;
        collectorRegistry.clear();

        // add all standard jvm collectors - memory, gc, machine, etc..
        DefaultExports.initialize();

        total_batches = Counter.build().labelNames(LABEL_NAME).name(SPARK_METRIC_PREFIX + "batches_total")
                .labelNames(LABEL_NAME).help("Total number of batches.").register();
        total_input_rows = Counter.build().name(SPARK_METRIC_PREFIX + "input_rows_total")
                .labelNames(LABEL_NAME).help("Total number of input events.").register();
        inputRowsPerSecond = Gauge.build().name(SPARK_METRIC_PREFIX + "input_rows_per_second")
                .labelNames(LABEL_NAME).help("Total number of input events.").register();
        processedRowsPerSecond = Gauge.build().name(SPARK_METRIC_PREFIX + "processed_rows_per_second")
                .labelNames(LABEL_NAME).help("Total number of input events.").register();

    }

    @Override
    public void onQueryStarted(QueryStartedEvent event) {

    }

    public void setAddSparkQueryStats(boolean addSparkQueryStats) {
        this.addSparkQueryStats = addSparkQueryStats;
    }

    public String getMetricsURLBase() {
        String base_url = "http://" + push_gateway_host + "/metrics/job/";
        //URLEncoder.encode(job_name, "UTF-8");
        return base_url;
    }

    @Override
    public void onQueryProgress(QueryProgressEvent event) {
        PushGateway pg = new PushGateway(push_gateway_host);
        try {
            total_batches.labels(app_name).inc();
            if (addSparkQueryStats) {
                total_input_rows.labels(app_name).inc(event.progress().numInputRows());
                processedRowsPerSecond.labels(app_name).set(event.progress().processedRowsPerSecond());
                inputRowsPerSecond.labels(app_name).set(event.progress().inputRowsPerSecond());
            }

            pg.pushAdd(collectorRegistry, job_name);
        } catch (IOException e) {
            LOGGER.error("Error in query progress statistic writing to " + getMetricsURLBase() +
                    " with error  \n" + e.getMessage());
        }

    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {

    }


}
