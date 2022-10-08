package io.siddhi.extension.io.live.metrics;

/**
 * Parent metrics class
 */
public class Metrics {
    protected final String siddhiAppName;

    protected Metrics(String siddhiAppName) {
        this.siddhiAppName = siddhiAppName;
    }
}