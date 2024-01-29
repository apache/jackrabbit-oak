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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsUtils {
    private final static Logger LOG = LoggerFactory.getLogger(MetricsUtils.class);

    /**
     * <p>Set a counter metric to the given value. The previous value of the metric is discarded and replaced by the
     * given value.</p>
     * <p>
     * Note that this method is to support a non-standard use case of the counter metrics.
     * Normally counters are incremented periodically to keep track of how many times an event occurred. This method
     * instead is intended to be called only once for a given metric, to provide the final value of the metrics. If
     * called more than once for the same metric, it will log a warning and discard the old value.
     * </p>
     *
     * @param statisticsProvider The statistics provider to use.
     * @param name               The name of the counter to set.
     * @param value              The value to set
     */
    public static void setCounterOnce(StatisticsProvider statisticsProvider, String name, long value) {
        CounterStats metric = statisticsProvider.getCounterStats(name, StatsOptions.METRICS_ONLY);
        LOG.debug("Setting counter {} to {}", name, value);
        if (metric.getCount() != 0) {
            LOG.warn("Counter was not 0: {} {}", name, metric.getCount());
            // Reset to 0
            metric.dec(metric.getCount());
        }
        metric.inc(value);
    }
}
