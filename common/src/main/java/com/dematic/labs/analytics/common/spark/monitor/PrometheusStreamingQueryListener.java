package com.dematic.labs.analytics.common.spark.monitor;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.spark.sql.streaming.StreamingQueryListener;

import java.io.IOException;

/**
 *
 * Uses prometheus java client https://github.com/prometheus/client_java
 *  to register structured streaming query stats
 */

public class PrometheusStreamingQueryListener extends StreamingQueryListener {
    private static String SPARK_METRIC_PREFIX="spark_structured_streaming_";
    private String push_gateway_host;
    private String job_name="push_gateway_test";

    private String app_name;
    CollectorRegistry collectorRegistry;

 // collectors for query
    private Counter total_batches;
    private Counter total_input_rows;
    private Gauge processedRowsPerSecond;
    private Gauge inputRowsPerSecond;
    private boolean addSparkQueryStats=true;


    public PrometheusStreamingQueryListener(String spark_app_name, String push_gateway_host){
        this.push_gateway_host = push_gateway_host;
        app_name=spark_app_name;
        collectorRegistry=CollectorRegistry.defaultRegistry;
        collectorRegistry.clear();

        // add all standard jvm collectors - memory, gc, machine, etc..
        DefaultExports.initialize();

        total_batches= Counter.build().name(SPARK_METRIC_PREFIX+"batches_total")
                .labelNames(app_name).help("Total number of batches.").register();
        total_input_rows= Counter.build().name(SPARK_METRIC_PREFIX+"input_rows_total")
                .labelNames(app_name).help("Total number of input events.").register();
        inputRowsPerSecond= Gauge.build().name(SPARK_METRIC_PREFIX+"input_rows_per_second")
                .labelNames(app_name).help("Total number of input events.").register();
        processedRowsPerSecond= Gauge.build().name(SPARK_METRIC_PREFIX+"processed_rows_per_second")
                .labelNames(app_name).help("Total number of input events.").register();

    }

    @Override
    public void onQueryStarted(QueryStartedEvent event) {

    }

    public void setAddSparkQueryStats(boolean addSparkQueryStats) {
        this.addSparkQueryStats = addSparkQueryStats;
    }

    @Override
    public void onQueryProgress(QueryProgressEvent event) {

        try {
            total_batches.labels("batches").inc();
            if (addSparkQueryStats) {
                total_input_rows.inc(event.progress().numInputRows());
                processedRowsPerSecond.set(event.progress().processedRowsPerSecond());
                inputRowsPerSecond.set(event.progress().inputRowsPerSecond());
            }

            PushGateway pg = new PushGateway(push_gateway_host);
            pg.pushAdd(collectorRegistry, job_name);
        } catch (IOException e) {

        }

    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {

    }


}
