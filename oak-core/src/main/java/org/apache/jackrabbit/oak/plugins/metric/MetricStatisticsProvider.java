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

import java.io.Closeable;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ObjectNameFactory;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Timer;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.api.stats.RepositoryStatistics.Type;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.SimpleStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.jackrabbit.stats.RepositoryStatisticsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricStatisticsProvider implements StatisticsProvider, Closeable {
    private static final Logger log = LoggerFactory.getLogger(MetricStatisticsProvider.class);

    private static final String JMX_TYPE_METRICS = "Metrics";

    private final Map<String, CompositeStats> statsRegistry = Maps.newHashMap();
    private final MetricRegistry registry;
    private final JmxReporter reporter;
    private final RepositoryStatisticsImpl repoStats;

    public MetricStatisticsProvider(MBeanServer server, ScheduledExecutorService executor) {
        registry = new MetricRegistry();
        repoStats = new RepositoryStatisticsImpl(executor);
        reporter = JmxReporter.forRegistry(registry)
                .inDomain(WhiteboardUtils.JMX_OAK_DOMAIN)
                .registerWith(server)
                .createsObjectNamesWith(new OakNameFactory())
                .build();
        reporter.start();

        registerAverages();
    }

    static String typeToName(Type type) {
        return type.name();
    }

    public void close() {
        if (reporter != null) {
            reporter.close();
        }
    }

    @Override
    public RepositoryStatistics getStats() {
        return repoStats;
    }

    @Override
    public MeterStats getMeter(String name) {
        return getStats(name, StatsType.METER);
    }

    @Override
    public CounterStats getCounterStats(String name) {
        return getStats(name, StatsType.COUNTER);
    }

    @Override
    public TimerStats getTimer(String name) {
        return getStats(name, StatsType.TIMER);
    }

    MetricRegistry getRegistry() {
        return registry;
    }

    RepositoryStatisticsImpl getRepoStats() {
        return repoStats;
    }

    private synchronized CompositeStats getStats(String type, StatsType statsType) {
        String name = type;
        Type enumType = Type.getType(type);
        CompositeStats stats = statsRegistry.get(type);
        if (stats == null) {
            SimpleStats delegate;
            if (enumType != null) {
                delegate = new SimpleStats(repoStats.getCounter(enumType));
                name = typeToName(enumType);
            } else {
                boolean resetValueEachSecond = statsType != StatsType.COUNTER;
                delegate = new SimpleStats(repoStats.getCounter(type, resetValueEachSecond));
            }
            stats = createStat(name, statsType, delegate);
            statsRegistry.put(type, stats);
        }
        return stats;
    }

    private CompositeStats createStat(String name, StatsType statsType, SimpleStats delegate) {
        switch (statsType) {
            case COUNTER:
                MetricCounterStats counter = new MetricCounterStats(registry.counter(name));
                return new CompositeStats(delegate, counter);
            case TIMER:
                MetricTimerStats timer = new MetricTimerStats(registry.timer(name));
                return new CompositeStats(delegate, timer);
            case METER:
                MetricMeterStats meter = new MetricMeterStats(registry.meter(name));
                return new CompositeStats(delegate, meter);
        }
        throw new IllegalStateException();
    }

    private void registerAverages() {
        registry.register(typeToName(Type.SESSION_READ_AVERAGE),
                new AvgGauge(registry.meter(typeToName(Type.SESSION_READ_COUNTER)),
                        registry.timer(typeToName(Type.SESSION_READ_DURATION))));

        registry.register(typeToName(Type.SESSION_WRITE_AVERAGE),
                new AvgGauge(registry.meter(typeToName(Type.SESSION_WRITE_COUNTER)),
                        registry.timer(typeToName(Type.SESSION_WRITE_DURATION))));

        registry.register(typeToName(Type.QUERY_AVERAGE),
                new AvgGauge(registry.meter(typeToName(Type.QUERY_COUNT)),
                        registry.timer(typeToName(Type.QUERY_DURATION))));

        registry.register(typeToName(Type.OBSERVATION_EVENT_AVERAGE),
                new AvgGauge(registry.meter(typeToName(Type.OBSERVATION_EVENT_COUNTER)),
                        registry.timer(typeToName(Type.OBSERVATION_EVENT_DURATION))));
    }

    private enum StatsType {METER, COUNTER, TIMER}

    private static class AvgGauge extends RatioGauge {
        private final Meter meter;
        private final Timer timer;

        private AvgGauge(Meter meter, Timer timer) {
            this.meter = meter;
            this.timer = timer;
        }

        @Override
        protected Ratio getRatio() {
            //TODO Should we use getMeanRate
            return Ratio.of(meter.getFifteenMinuteRate(),
                    timer.getFifteenMinuteRate());
        }
    }

    private static class OakNameFactory implements ObjectNameFactory {
        @Override
        public ObjectName createName(String type, String domain, String name) {
            Hashtable<String, String> table = new Hashtable<String, String>();
            table.put("type", JMX_TYPE_METRICS);
            table.put("name", name);
            try {
                return new ObjectName(domain, table);
            } catch (MalformedObjectNameException e) {
                log.warn("Unable to register {} {}", type, name, e);
                throw new RuntimeException(e);
            }
        }
    }

}
