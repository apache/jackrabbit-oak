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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
import org.apache.jackrabbit.oak.commons.jmx.JmxUtil;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.SimpleStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.Stats;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.jackrabbit.stats.RepositoryStatisticsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricStatisticsProvider implements StatisticsProvider, Closeable {
    private static final Logger log = LoggerFactory.getLogger(MetricStatisticsProvider.class);

    private static final String JMX_TYPE_METRICS = "Metrics";

    private final ConcurrentMap<String, Stats> statsRegistry = Maps.newConcurrentMap();
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
    public MeterStats getMeter(String name, StatsOptions options) {
        return getStats(name, StatsBuilder.METERS, options);
    }

    @Override
    public CounterStats getCounterStats(String name, StatsOptions options) {
        return getStats(name, StatsBuilder.COUNTERS, options);
    }

    @Override
    public TimerStats getTimer(String name, StatsOptions options) {
        return getStats(name, StatsBuilder.TIMERS, options);
    }

    @Override
    public HistogramStats getHistogram(String name, StatsOptions options) {
        return getStats(name, StatsBuilder.HISTOGRAMS, options);
    }

    public MetricRegistry getRegistry() {
        return registry;
    }

    RepositoryStatisticsImpl getRepoStats() {
        return repoStats;
    }

    private <T extends Stats> T getStats(String name, StatsBuilder<T> builder, StatsOptions options) {
        Stats stats = statsRegistry.get(name);
        //Use double locking pattern. The map should get populated with required set
        //during startup phase so for later calls should not hit the synchronized block
        if (stats == null) {
            synchronized (this){
                stats = statsRegistry.get(name);
                //If still null then go ahead and create it within lock
                if (stats == null){
                    if (options.isOnlyMetricEnabled()) {
                        stats = builder.newMetric(this, name);
                    } else if (options.isOnlyTimeSeriesEnabled()) {
                        stats = new SimpleStats(getTimerSeriesStats(name, builder), builder.getType());
                    } else {
                        stats = builder.newComposite(getTimerSeriesStats(name, builder), this, name);
                    }
                    statsRegistry.put(name, stats);
                }
            }
        }

        if (builder.isInstance(stats)) {
            //noinspection unchecked
            return (T) stats;
        }

        throw new IllegalStateException();
    }

    private AtomicLong getTimerSeriesStats(String name, StatsBuilder builder){
        AtomicLong counter;
        Type enumType = Type.getType(name);
        if (enumType != null) {
            counter = repoStats.getCounter(enumType);
        } else {
            boolean resetValueEachSecond = builder != StatsBuilder.COUNTERS;
            counter = repoStats.getCounter(name, resetValueEachSecond);
        }
        return counter;
    }

    private void registerAverages() {
        registry.register(typeToName(Type.OBSERVATION_EVENT_AVERAGE),
                new AvgGauge(compStats(Type.OBSERVATION_EVENT_COUNTER, StatsBuilder.METERS).getMeter(),
                        compStats(Type.OBSERVATION_EVENT_DURATION, StatsBuilder.TIMERS).getTimer()));
    }

    private CompositeStats compStats(Type type, StatsBuilder builder){
        Stats stats = getStats(typeToName(type), builder, StatsOptions.DEFAULT);
        return (CompositeStats) stats;
    }

    @SuppressWarnings("unused")
    private interface StatsBuilder<T extends Stats> {
        StatsBuilder<CounterStats> COUNTERS = new StatsBuilder<CounterStats>() {
            @Override
            public CompositeStats newComposite(AtomicLong delegate, MetricStatisticsProvider provider, String name) {
                return new CompositeStats(delegate, provider.registry.counter(name));
            }

            @Override
            public Stats newMetric(MetricStatisticsProvider provider, String name) {
                return new CounterImpl(provider.registry.counter(name));
            }

            @Override
            public boolean isInstance(Stats metric) {
                return CounterStats.class.isInstance(metric);
            }

            @Override
            public SimpleStats.Type getType() {
                return SimpleStats.Type.COUNTER;
            }
        };

        StatsBuilder<MeterStats> METERS = new StatsBuilder<MeterStats>() {
            @Override
            public CompositeStats newComposite(AtomicLong delegate, MetricStatisticsProvider provider, String name) {
                return new CompositeStats(delegate, getMeter(provider, name));
            }

            @Override
            public Stats newMetric(MetricStatisticsProvider provider, String name) {
                return new MeterImpl(getMeter(provider, name));
            }

            @Override
            public boolean isInstance(Stats metric) {
                return MeterStats.class.isInstance(metric);
            }

            @Override
            public SimpleStats.Type getType() {
                return SimpleStats.Type.METER;
            }

            private Meter getMeter(MetricStatisticsProvider provider, String name) {
                Meter meter = new Meter(provider.metricsClock);
                provider.registry.register(name, meter);
                return meter;
            }
        };

        StatsBuilder<TimerStats> TIMERS = new StatsBuilder<TimerStats>() {

            @Override
            public CompositeStats newComposite(AtomicLong delegate, MetricStatisticsProvider provider, String name) {
                return new CompositeStats(delegate, provider.registry.timer(name));
            }

            @Override
            public Stats newMetric(MetricStatisticsProvider provider, String name) {
                return new TimerImpl(provider.registry.timer(name));
            }

            @Override
            public boolean isInstance(Stats metric) {
                return TimerStats.class.isInstance(metric);
            }

            @Override
            public SimpleStats.Type getType() {
                return SimpleStats.Type.TIMER;
            }
        };

        StatsBuilder<HistogramStats> HISTOGRAMS = new StatsBuilder<HistogramStats>() {

            @Override
            public CompositeStats newComposite(AtomicLong delegate, MetricStatisticsProvider provider, String name) {
                return new CompositeStats(delegate, provider.registry.histogram(name));
            }

            @Override
            public Stats newMetric(MetricStatisticsProvider provider, String name) {
                return new HistogramImpl(provider.registry.histogram(name));
            }

            @Override
            public boolean isInstance(Stats metric) {
                return HistogramStats.class.isInstance(metric);
            }

            @Override
            public SimpleStats.Type getType() {
                return SimpleStats.Type.HISTOGRAM;
            }
        };

        CompositeStats newComposite(AtomicLong delegate, MetricStatisticsProvider provider, String name);

        Stats newMetric(MetricStatisticsProvider provider,String name);

        boolean isInstance(Stats stats);

        SimpleStats.Type getType();
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
            table.put("name", JmxUtil.quoteValueIfRequired(name));
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
