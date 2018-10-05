/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.metric;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;

import com.codahale.metrics.JmxReporter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.jackrabbit.api.stats.RepositoryStatistics.Type;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.NoopStats;
import org.apache.jackrabbit.oak.stats.SimpleStats;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MetricStatisticsProviderTest {
    private MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private MetricStatisticsProvider statsProvider = new MetricStatisticsProvider(server, executorService);

    @Test
    public void basicSetup() throws Exception {
        //By default avg counters would be configured. So check if they are
        //configured
        assertEquals(1, statsProvider.getRegistry().getMeters().size());
        assertEquals(1, statsProvider.getRegistry().getTimers().size());

        assertNotNull(statsProvider.getStats());
        assertEquals(statsProvider.getRegistry().getMetrics().size(), getMetricMbeans().size());
        statsProvider.close();

        assertEquals(0, getMetricMbeans().size());
    }

    @Test
    public void meter() throws Exception {
        MeterStats meterStats = statsProvider.getMeter("test", StatsOptions.DEFAULT);

        assertNotNull(meterStats);
        assertNotNull(statsProvider.getRegistry().getMeters().containsKey("test"));
        assertTrue(((CompositeStats) meterStats).isMeter());
    }

    @Test
    public void counter() throws Exception {
        CounterStats counterStats = statsProvider.getCounterStats("test", StatsOptions.DEFAULT);

        assertNotNull(counterStats);
        assertNotNull(statsProvider.getRegistry().getCounters().containsKey("test"));
        assertTrue(((CompositeStats) counterStats).isCounter());
    }

    @Test
    public void timer() throws Exception {
        TimerStats timerStats = statsProvider.getTimer("test", StatsOptions.DEFAULT);

        assertNotNull(timerStats);
        assertNotNull(statsProvider.getRegistry().getTimers().containsKey("test"));
        assertTrue(((CompositeStats) timerStats).isTimer());
    }

    @Test
    public void histogram() throws Exception {
        HistogramStats histoStats = statsProvider.getHistogram("test", StatsOptions.DEFAULT);

        assertNotNull(histoStats);
        assertNotNull(statsProvider.getRegistry().getHistograms().containsKey("test"));
        assertTrue(((CompositeStats) histoStats).isHistogram());
    }

    @Test
    public void timeSeriesIntegration() throws Exception {
        MeterStats meterStats = statsProvider.getMeter(Type.SESSION_COUNT.name(), StatsOptions.DEFAULT);

        meterStats.mark(5);
        assertEquals(5, statsProvider.getRepoStats().getCounter(Type.SESSION_COUNT).get());
    }

    @Test
    public void jmxNaming() throws Exception {
        TimerStats timerStats = statsProvider.getTimer("hello", StatsOptions.DEFAULT);
        assertNotNull(server.getObjectInstance(new ObjectName("org.apache.jackrabbit.oak:type=Metrics,name=hello")));
    }

    @Test
    public void noopMeter() throws Exception {
        assertInstanceOf(statsProvider.getTimer(Type.SESSION_READ_DURATION.name(), StatsOptions.TIME_SERIES_ONLY), SimpleStats.class);
        assertNotEquals(statsProvider.getMeter(Type.OBSERVATION_EVENT_COUNTER.name(), StatsOptions.TIME_SERIES_ONLY), NoopStats.INSTANCE);
    }

    @Test
    public void statsOptions_MetricOnly() throws Exception{
        assertInstanceOf(statsProvider.getTimer("fooTimer", StatsOptions.METRICS_ONLY), TimerImpl.class);
        assertInstanceOf(statsProvider.getCounterStats("fooCounter", StatsOptions.METRICS_ONLY), CounterImpl.class);
        assertInstanceOf(statsProvider.getMeter("fooMeter", StatsOptions.METRICS_ONLY), MeterImpl.class);
        assertInstanceOf(statsProvider.getHistogram("fooHisto", StatsOptions.METRICS_ONLY), HistogramImpl.class);
    }

    @Test
    public void statsOptions_TimeSeriesOnly() throws Exception{
        assertInstanceOf(statsProvider.getTimer("fooTimer", StatsOptions.TIME_SERIES_ONLY), SimpleStats.class);
    }

    @Test
    public void statsOptions_Default() throws Exception{
        assertInstanceOf(statsProvider.getTimer("fooTimer", StatsOptions.DEFAULT), CompositeStats.class);
    }

    @Test
    public void concurrentAccess() throws Exception{
        //Queue is used to collect instances with minimal overhead in concurrent scenario
        final Queue<MeterStats> statsQueue = new ConcurrentLinkedDeque<MeterStats>();
        List<Thread> threads = Lists.newArrayList();
        int numWorker = 5;
        final CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < numWorker; i++) {
            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    Uninterruptibles.awaitUninterruptibly(latch);
                    statsQueue.add(statsProvider.getMeter("foo", StatsOptions.DEFAULT));
                }
            }));
        }

        for (Thread t : threads){
            t.start();
        }

        latch.countDown();

        for (Thread t : threads){
            t.join();
        }

        //Assert that we get same reference for every call
        Set<MeterStats> statsSet = Sets.newIdentityHashSet();

        for (MeterStats m : statsQueue){
            statsSet.add(m);
        }

        assertEquals(1, statsSet.size());

    }

    @After
    public void cleanup() {
        statsProvider.close();
        executorService.shutdownNow();
    }

    private Set<ObjectInstance> getMetricMbeans() throws MalformedObjectNameException {
        QueryExp q = Query.isInstanceOf(Query.value(JmxReporter.MetricMBean.class.getName()));
        return server.queryMBeans(new ObjectName("org.apache.jackrabbit.oak:*"), q);
    }

    private void assertInstanceOf(Object o, Class<?> clazz){
        if (!clazz.isInstance(o)){
            fail(String.format("%s is not an instance of %s", o.getClass(), clazz));
        }
    }

}
