package io.siddhi.extension.io.live.metrics;

import org.wso2.carbon.metrics.core.Counter;
import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

public class SourceMetrics extends Metrics{

    public SourceMetrics(String siddhiAppName) {
        super(siddhiAppName);
    }

//     To count the total reads from siddhi app level
    public Counter getTotalReadsMetric() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Total.Reads.%s",
                        siddhiAppName, "live"), Level.INFO);
    }

}
