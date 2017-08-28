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
package org.apache.jackrabbit.oak.cache.impl;

import java.util.List;
import java.util.Map;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OSGi component that binds to all {@link CacheStatsMBean} instances and
 * exposes their counters as {@link Metric}s.
 */
@Component(immediate = true)
public class CacheStatsMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(CacheStatsMetrics.class);

    static final String HIT = "hit";
    static final String MISS = "miss";
    static final String EVICTION = "eviction";
    static final String ELEMENT = "element";
    static final String LOAD_TIME = "loadTime";

    private static final List<String> TYPES = ImmutableList.of(
            HIT, MISS, EVICTION, ELEMENT, LOAD_TIME);

    private Map<String, CacheStatsMBean> cacheStatsMBeans = Maps.newHashMap();
    private MetricRegistry registry = new MetricRegistry();

    @Reference(
            cardinality = ReferenceCardinality.AT_LEAST_ONE,
            policy = ReferencePolicy.DYNAMIC
    )
    synchronized void addCacheStatsMBean(CacheStatsMBean stats) {
        LOG.debug("addCacheStatsMBean({})", stats.getName());
        cacheStatsMBeans.put(stats.getName(), stats);
        registerCacheStatsMBean(this.registry, stats);
    }

    synchronized void removeCacheStatsMBean(CacheStatsMBean stats) {
        LOG.debug("removeCacheStatsMBean({})", stats.getName());
        cacheStatsMBeans.remove(stats.getName());
        unregisterCacheStatsMBean(this.registry, stats);
    }

    @Reference(
            target = "(name=oak)"
    )
    synchronized void setMetricRegistry(MetricRegistry registry) {
        LOG.debug("setMetricRegistry({})", registry);
        for (CacheStatsMBean stats : cacheStatsMBeans.values()) {
            unregisterCacheStatsMBean(this.registry, stats);
            registerCacheStatsMBean(registry, stats);
        }
        this.registry = registry;
    }

    private static void registerCacheStatsMBean(MetricRegistry registry,
                                                CacheStatsMBean stats) {
        registerMetric(registry, new HitCounter(stats));
        registerMetric(registry, new MissCounter(stats));
        registerMetric(registry, new EvictionCounter(stats));
        registerMetric(registry, new ElementCounter(stats));
        registerMetric(registry, new LoadTimeCounter(stats));
    }

    private static void registerMetric(MetricRegistry registry,
                                       CacheStatsMBeanCounter metric) {
        String name = metric.getName();
        registry.remove(name);
        registry.register(name, metric);
    }

    private static void unregisterCacheStatsMBean(MetricRegistry registry,
                                                  CacheStatsMBean stats) {
        String name = stats.getName();
        for (String t : TYPES) {
            registry.remove(metricName(name, t));
        }
    }

    static String metricName(String cacheStatsName, String type) {
        return "CacheStats." + cacheStatsName + "." + type;
    }

    private abstract static class CacheStatsMBeanCounter
            extends Counter {

        protected CacheStatsMBean stats;
        protected String type;

        CacheStatsMBeanCounter(CacheStatsMBean stats, String type) {
            this.stats = stats;
            this.type = type;
        }

        String getName() {
            return metricName(stats.getName(), type);
        }
    }

    private static final class HitCounter extends CacheStatsMBeanCounter {

        HitCounter(CacheStatsMBean stats) {
            super(stats, HIT);
        }

        @Override
        public long getCount() {
            return stats.getHitCount();
        }
    }

    private static final class MissCounter extends CacheStatsMBeanCounter {

        MissCounter(CacheStatsMBean stats) {
            super(stats, MISS);
        }

        @Override
        public long getCount() {
            return stats.getMissCount();
        }
    }

    private static final class EvictionCounter extends CacheStatsMBeanCounter {

        EvictionCounter(CacheStatsMBean stats) {
            super(stats, EVICTION);
        }

        @Override
        public long getCount() {
            return stats.getEvictionCount();
        }
    }

    private static final class ElementCounter extends CacheStatsMBeanCounter {

        ElementCounter(CacheStatsMBean stats) {
            super(stats, ELEMENT);
        }

        @Override
        public long getCount() {
            return stats.getElementCount();
        }
    }

    private static final class LoadTimeCounter extends CacheStatsMBeanCounter {

        LoadTimeCounter(CacheStatsMBean stats) {
            super(stats, LOAD_TIME);
        }

        @Override
        public long getCount() {
            return stats.getTotalLoadTime();
        }
    }
}
