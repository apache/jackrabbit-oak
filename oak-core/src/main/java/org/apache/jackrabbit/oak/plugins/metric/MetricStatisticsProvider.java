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
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ObjectNameFactory;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.api.stats.RepositoryStatistics.Type;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.SimpleStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.Stats;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.jackrabbit.stats.RepositoryStatisticsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricStatisticsProvider implements StatisticsProvider, Closeable {
    private static final Logger log = LoggerFactory.getLogger(MetricStatisticsProvider.class);

    private static final String JMX_TYPE_METRICS = "Metrics";

    /**
     * Types for which Noop Variant has to be used
     */
    private static final Set<String> NOOPS_TYPES = ImmutableSet.of(
            Type.SESSION_READ_DURATION.name(),
            Type.SESSION_WRITE_DURATION.name(),
            Type.QUERY_COUNT.name()
    );

    private final Map<String, Stats> statsRegistry = Maps.newHashMap();
    private final MetricRegistry registry;
    private final JmxReporter reporter;
    private final RepositoryStatisticsImpl repoStats;
    private final Clock.Fast clock;
    private final com.codahale.metrics.Clock metricsClock;

    public MetricStatisticsProvider(MBeanServer server, ScheduledExecutorService executor) {
        clock = new Clock.Fast(executor);
        metricsClock = new OakMetricClock(clock);
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
        clock.close();
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
        return getStats(name, StatsBuilder.METERS);
    }

    @Override
    public CounterStats getCounterStats(String name) {
        return getStats(name, StatsBuilder.COUNTERS);
    }

    @Override
    public TimerStats getTimer(String name) {
        return getStats(name, StatsBuilder.TIMERS);
    }

    public MetricRegistry getRegistry() {
        return registry;
    }

    RepositoryStatisticsImpl getRepoStats() {
        return repoStats;
    }

    private synchronized <T extends Stats> T getStats(String type, StatsBuilder<T> builder) {
        String name = type;
        Type enumType = Type.getType(type);
        Stats stats = statsRegistry.get(type);
        if (stats == null) {
            SimpleStats delegate;
            if (enumType != null) {
                delegate = new SimpleStats(repoStats.getCounter(enumType));
                name = typeToName(enumType);
            } else {
                boolean resetValueEachSecond = builder != StatsBuilder.COUNTERS;
                delegate = new SimpleStats(repoStats.getCounter(type, resetValueEachSecond));
            }

            if (NOOPS_TYPES.contains(name)) {
                stats = delegate;
            } else {
                stats = builder.newComposite(delegate, this, name);
            }

            statsRegistry.put(type, stats);
        }

        if (builder.isInstance(stats)) {
            //noinspection unchecked
            return (T) stats;
        }

        throw new IllegalStateException();
    }

    private void registerAverages() {
        registry.register(typeToName(Type.OBSERVATION_EVENT_AVERAGE),
                new AvgGauge(compStats(Type.OBSERVATION_EVENT_COUNTER, StatsBuilder.METERS).getMeter(),
                        compStats(Type.OBSERVATION_EVENT_DURATION, StatsBuilder.TIMERS).getTimer()));
    }

    private CompositeStats compStats(Type type, StatsBuilder builder){
        Stats stats = getStats(typeToName(type), builder);
        return (CompositeStats) stats;
    }

    @SuppressWarnings("unused")
    private interface StatsBuilder<T extends Stats> {
        StatsBuilder<CounterStats> COUNTERS = new StatsBuilder<CounterStats>() {
            @Override
            public CompositeStats newComposite(SimpleStats delegate, MetricStatisticsProvider provider,String name) {
                return new CompositeStats(delegate, provider.registry.counter(name));
            }

            @Override
            public boolean isInstance(Stats metric) {
                return CounterStats.class.isInstance(metric);
            }
        };

        StatsBuilder<MeterStats> METERS = new StatsBuilder<MeterStats>() {
            @Override
            public CompositeStats newComposite(SimpleStats delegate, MetricStatisticsProvider provider,String name) {
                Meter meter = new Meter(provider.metricsClock);
                provider.registry.register(name, meter);
                return new CompositeStats(delegate, meter);
            }

            @Override
            public boolean isInstance(Stats metric) {
                return MeterStats.class.isInstance(metric);
            }
        };

        StatsBuilder<TimerStats> TIMERS = new StatsBuilder<TimerStats>() {

            @Override
            public CompositeStats newComposite(SimpleStats delegate, MetricStatisticsProvider provider,String name) {
                return new CompositeStats(delegate, provider.registry.timer(name));
            }

            @Override
            public boolean isInstance(Stats metric) {
                return TimerStats.class.isInstance(metric);
            }
        };

        CompositeStats newComposite(SimpleStats delegate, MetricStatisticsProvider provider,String name);

        boolean isInstance(Stats stats);
    }

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

    private static class OakMetricClock extends com.codahale.metrics.Clock {
        private final Clock clock;

        public OakMetricClock(Clock clock) {
            this.clock = clock;
        }

        @Override
        public long getTick() {
            return TimeUnit.NANOSECONDS.convert(clock.getTime(), TimeUnit.MILLISECONDS);
        }
    }

}
