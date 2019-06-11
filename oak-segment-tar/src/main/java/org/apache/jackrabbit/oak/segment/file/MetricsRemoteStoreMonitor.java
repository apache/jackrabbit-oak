package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.jetbrains.annotations.NotNull;
import java.util.concurrent.TimeUnit;

public class MetricsRemoteStoreMonitor extends RemoteStoreMonitorAdapter {

    public static final String REQUEST_COUNT = "REQUEST_COUNT";
    public static final String REQUEST_ERROR = "REQUEST_ERROR";
    public static final String REQUEST_DURATION = "REQUEST_DURATION";

    private final CounterStats requestCountStats;
    private final CounterStats requestErrorStats;
    private final TimerStats requestDurationStats;

    public MetricsRemoteStoreMonitor(@NotNull StatisticsProvider statisticsProvider) {
        requestCountStats = statisticsProvider.getCounterStats(REQUEST_COUNT, StatsOptions.DEFAULT);
        requestErrorStats = statisticsProvider.getCounterStats(REQUEST_ERROR, StatsOptions.DEFAULT);
        requestDurationStats = statisticsProvider.getTimer(REQUEST_DURATION, StatsOptions.METRICS_ONLY);
    }

    @Override
    public void requestCount() {
        requestCountStats.inc(); }

    @Override
    public void requestError() {
        requestErrorStats.inc();
    }

    @Override
    public void requestDuration(long duration, TimeUnit timeUnit) {
        requestDurationStats.update(duration, timeUnit);
    }
}
