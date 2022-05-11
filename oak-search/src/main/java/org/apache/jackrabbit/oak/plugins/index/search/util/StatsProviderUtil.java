package org.apache.jackrabbit.oak.plugins.index.search.util;

import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.function.BiFunction;

public class StatsProviderUtil {

    private final StatisticsProvider statisticsProvider;
    private final BiFunction<String, Map<String, String>, String> metricName;

    public StatsProviderUtil(@NotNull StatisticsProvider statisticsProvider) {
        metricName = (name, labels) -> labels.entrySet().stream().reduce(name,
                (n, e) -> n + ";" + e.getKey() + "=" + e.getValue(),
                (n1, n2) -> n1 + n2);
        this.statisticsProvider = statisticsProvider;
    }

    public BiFunction<String, Map<String, String>, HistogramStats> getHistoStats() {
        return (name, labels) -> statisticsProvider.getHistogram(metricName.apply(name, labels), StatsOptions.METRICS_ONLY);
    }

    public BiFunction<String, Map<String, String>, CounterStats> getCounterStats() {
        return (name, labels) -> statisticsProvider.getCounterStats(metricName.apply(name, labels), StatsOptions.METRICS_ONLY);
    }

    public BiFunction<String, Map<String, String>, TimerStats> getTimerStats() {
        return (name, labels) -> statisticsProvider.getTimer(metricName.apply(name, labels), StatsOptions.METRICS_ONLY);
    }

    public BiFunction<String, Map<String, String>, MeterStats> getMeterStats() {
        return (name, labels) -> statisticsProvider.getMeter(metricName.apply(name, labels), StatsOptions.METRICS_ONLY);
    }

}
