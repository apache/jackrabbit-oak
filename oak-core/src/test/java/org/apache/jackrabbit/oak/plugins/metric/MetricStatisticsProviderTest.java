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
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;

import com.codahale.metrics.JmxReporter;
import org.apache.jackrabbit.api.stats.RepositoryStatistics.Type;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.NoopStats;
import org.apache.jackrabbit.oak.stats.SimpleStats;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MetricStatisticsProviderTest {
    private MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private MetricStatisticsProvider statsProvider;

    @Test
    public void basicSetup() throws Exception {
        statsProvider = new MetricStatisticsProvider(server, executorService);

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
        statsProvider = new MetricStatisticsProvider(server, executorService);
        MeterStats meterStats = statsProvider.getMeter("test");

        assertNotNull(meterStats);
        assertNotNull(statsProvider.getRegistry().getMeters().containsKey("test"));
        assertTrue(((CompositeStats) meterStats).isMeter());
    }

    @Test
    public void counter() throws Exception {
        statsProvider = new MetricStatisticsProvider(server, executorService);
        CounterStats counterStats = statsProvider.getCounterStats("test");

        assertNotNull(counterStats);
        assertNotNull(statsProvider.getRegistry().getCounters().containsKey("test"));
        assertTrue(((CompositeStats) counterStats).isCounter());
    }

    @Test
    public void timer() throws Exception {
        statsProvider = new MetricStatisticsProvider(server, executorService);
        TimerStats timerStats = statsProvider.getTimer("test");

        assertNotNull(timerStats);
        assertNotNull(statsProvider.getRegistry().getTimers().containsKey("test"));
        assertTrue(((CompositeStats) timerStats).isTimer());
    }

    @Test
    public void timeSeriesIntegration() throws Exception {
        statsProvider = new MetricStatisticsProvider(server, executorService);
        MeterStats meterStats = statsProvider.getMeter(Type.SESSION_COUNT.name());

        meterStats.mark(5);
        assertEquals(5, statsProvider.getRepoStats().getCounter(Type.SESSION_COUNT).get());
    }

    @Test
    public void jmxNaming() throws Exception {
        statsProvider = new MetricStatisticsProvider(server, executorService);
        TimerStats timerStats = statsProvider.getTimer("hello");
        assertNotNull(server.getObjectInstance(new ObjectName("org.apache.jackrabbit.oak:type=Metrics,name=hello")));
    }

    @Test
    public void noopMeter() throws Exception {
        statsProvider = new MetricStatisticsProvider(server, executorService);
        assertTrue(statsProvider.getTimer(Type.SESSION_READ_DURATION.name()) instanceof SimpleStats);
        assertTrue(statsProvider.getTimer(Type.SESSION_WRITE_DURATION.name()) instanceof SimpleStats);
        assertTrue(statsProvider.getTimer(Type.QUERY_COUNT.name()) instanceof SimpleStats);
        assertNotEquals(statsProvider.getMeter(Type.OBSERVATION_EVENT_COUNTER.name()), NoopStats.INSTANCE);
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

}
