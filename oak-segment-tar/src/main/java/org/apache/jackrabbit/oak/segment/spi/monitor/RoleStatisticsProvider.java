/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.jackrabbit.oak.segment.spi.monitor;

import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;

public class RoleStatisticsProvider implements StatisticsProvider{

    private final StatisticsProvider delegate;
    private final String role;
    private final RepositoryStatistics repositoryStatistics;

    public RoleStatisticsProvider(StatisticsProvider delegate, String role) {
        this.delegate = delegate;
        this.role = role;

        this.repositoryStatistics = new RepositoryStatistics() {

            @Override
            public TimeSeries getTimeSeries(Type type) {
                return getTimeSeries(type.name(), type.isResetValueEachSecond());
            }

            @Override
            public TimeSeries getTimeSeries(String type, boolean resetValueEachSecond) {
                return delegate.getStats().getTimeSeries(addRoleToName(type, role), resetValueEachSecond);
            }
        };
    }

    @Override
    public RepositoryStatistics getStats() {
        return repositoryStatistics;
    }

    @Override
    public MeterStats getMeter(String name, StatsOptions options) {
        return delegate.getMeter(addRoleToName(name, role), options);
    }

    @Override
    public CounterStats getCounterStats(String name, StatsOptions options) {
        return delegate.getCounterStats(addRoleToName(name, role), options);
    }

    @Override
    public TimerStats getTimer(String name, StatsOptions options) {
        return delegate.getTimer(addRoleToName(name, role), options);
    }

    @Override
    public HistogramStats getHistogram(String name, StatsOptions options) {
        return delegate.getHistogram(addRoleToName(name, role), options);
    }

    private static String addRoleToName(String name, String role) {
        return role + '.' + name;
    }
}
