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

package org.apache.jackrabbit.oak.stats;

import org.osgi.annotation.versioning.ProviderType;
import org.apache.jackrabbit.api.stats.RepositoryStatistics;

@ProviderType
public interface StatisticsProvider {
    StatisticsProvider NOOP = new StatisticsProvider() {
        @Override
        public RepositoryStatistics getStats() {
            return NoopRepositoryStatistics.INSTANCE;
        }

        @Override
        public MeterStats getMeter(String name, StatsOptions options) {
            return NoopStats.INSTANCE;
        }

        @Override
        public CounterStats getCounterStats(String name, StatsOptions options) {
            return NoopStats.INSTANCE;
        }

        @Override
        public TimerStats getTimer(String name, StatsOptions options) {
            return NoopStats.INSTANCE;
        }

        @Override
        public HistogramStats getHistogram(String name, StatsOptions options) {
            return NoopStats.INSTANCE;
        }
    };


    RepositoryStatistics getStats();

    MeterStats getMeter(String name, StatsOptions options);

    CounterStats getCounterStats(String name, StatsOptions options);

    TimerStats getTimer(String name, StatsOptions options);

    HistogramStats getHistogram(String name, StatsOptions options);
}
