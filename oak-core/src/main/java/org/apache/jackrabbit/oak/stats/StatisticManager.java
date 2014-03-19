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

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.api.jmx.QueryStatManagerMBean;
import org.apache.jackrabbit.api.stats.RepositoryStatistics.Type;
import org.apache.jackrabbit.oak.api.jmx.RepositoryStatsMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.stats.QueryStatImpl;
import org.apache.jackrabbit.stats.RepositoryStatisticsImpl;
import org.apache.jackrabbit.stats.jmx.QueryStatManager;

/**
 * Manager for all repository wide statistics.
 * @see org.apache.jackrabbit.api.stats.RepositoryStatistics
 * @see org.apache.jackrabbit.api.stats.QueryStat
 */
public class StatisticManager {
    private final QueryStatImpl queryStat = new QueryStatImpl();
    private final RepositoryStatisticsImpl repoStats;
    private final TimeSeriesMax maxQueueLength;
    private final CompositeRegistration registration;

    /**
     * Create a new instance of this class registering all repository wide
     * statistics with the passed {@code whiteboard}.
     * @param whiteboard   whiteboard for registering the individual statistics with
     */
    public StatisticManager(Whiteboard whiteboard, ScheduledExecutorService executor) {
        repoStats = new RepositoryStatisticsImpl(executor);
        maxQueueLength = new TimeSeriesMax(executor);
        registration = new CompositeRegistration(
            registerMBean(whiteboard, QueryStatManagerMBean.class, new QueryStatManager(queryStat),
                    "QueryStat", "Oak Query Statistics"),
            registerMBean(whiteboard, RepositoryStatsMBean.class, new RepositoryStats(repoStats, maxQueueLength),
                    RepositoryStats.TYPE, "Oak Repository Statistics"));
    }

    /**
     * Logs the call of each query ran on the repository.
     * @param language   the query language
     * @param statement  the query
     * @param millis     time it took to evaluate the query in milli seconds.
     * @see org.apache.jackrabbit.stats.QueryStatCore#logQuery(java.lang.String, java.lang.String, long)
     */
    public void logQueryEvaluationTime(String language, String statement, long millis) {
        queryStat.logQuery(language, statement, millis);
    }

    /**
     * Get the counter of the specified {@code type}.
     * @param type  type of the counter
     * @return  counter for the given {@code type}
     * @see org.apache.jackrabbit.stats.RepositoryStatisticsImpl#getCounter(org.apache.jackrabbit.api.stats.RepositoryStatistics.Type)
     */
    public AtomicLong getCounter(Type type) {
        return repoStats.getCounter(type);
    }

    public TimeSeriesMax maxQueLengthRecorder() {
        return maxQueueLength;
    }

    /**
     * Unregister all statistics previously registered with the whiteboard passed
     * to the constructor.
     */
    public void dispose() {
        registration.unregister();
    }

}
