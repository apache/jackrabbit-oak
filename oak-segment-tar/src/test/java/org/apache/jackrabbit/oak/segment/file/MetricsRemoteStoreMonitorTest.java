package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.After;

import static org.apache.jackrabbit.oak.segment.file.MetricsRemoteStoreMonitor.*;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricsRemoteStoreMonitorTest {
    private ScheduledExecutorService executor;

    private CounterStats requestCount;
    private CounterStats requestErrorCount;
    private TimerStats requestDuration;

    private int requestCountExpected = 3;
    private int requestErrorCountExpected = 2;

    @Before
    public void setup(){
        executor = Executors.newSingleThreadScheduledExecutor();
        DefaultStatisticsProvider statisticsProvider = new DefaultStatisticsProvider(executor);
        MetricsRemoteStoreMonitor remoteStoreMonitor = new MetricsRemoteStoreMonitor(statisticsProvider);
        requestCount = statisticsProvider.getCounterStats(REQUEST_COUNT, StatsOptions.DEFAULT);
        requestErrorCount = statisticsProvider.getCounterStats(REQUEST_ERROR, StatsOptions.DEFAULT);
        requestDuration =  statisticsProvider.getTimer(REQUEST_DURATION, StatsOptions.METRICS_ONLY);

        for(int i = 0; i < requestCountExpected; i++){
            remoteStoreMonitor.requestCount();
        }
        for(int i = 0; i < requestErrorCountExpected; i++){
            requestErrorCount.inc();
        }
        requestDuration.update(100, TimeUnit.MILLISECONDS);

    }

    @After
    public void tearDown(){
        new ExecutorCloser(executor).close();
    }

    @Test
    public void testStats(){
        assertEquals(requestCountExpected, requestCount.getCount());
        assertEquals(requestErrorCountExpected, requestErrorCount.getCount());
        assertEquals(1, requestDuration.getCount());
    }
}
