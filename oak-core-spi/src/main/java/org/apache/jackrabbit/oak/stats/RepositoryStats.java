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

import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.OBSERVATION_EVENT_AVERAGE;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.OBSERVATION_EVENT_COUNTER;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.OBSERVATION_EVENT_DURATION;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.QUERY_AVERAGE;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.QUERY_COUNT;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.QUERY_DURATION;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.SESSION_COUNT;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.SESSION_LOGIN_COUNTER;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.SESSION_READ_AVERAGE;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.SESSION_READ_COUNTER;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.SESSION_READ_DURATION;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.SESSION_WRITE_AVERAGE;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.SESSION_WRITE_COUNTER;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.SESSION_WRITE_DURATION;

import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.api.jmx.RepositoryStatsMBean;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.stats.TimeSeriesStatsUtil;

public class RepositoryStats extends AnnotatedStandardMBean implements RepositoryStatsMBean {

    static final String OBSERVATION_QUEUE_MAX_LENGTH = "OBSERVATION_QUEUE_MAX_LENGTH";

    private final RepositoryStatistics repoStats;
    private final TimeSeries maxQueueLength;

    public RepositoryStats(RepositoryStatistics repoStats, TimeSeries maxQueueLength) {
        super(RepositoryStatsMBean.class);
        this.repoStats = repoStats;
        this.maxQueueLength = maxQueueLength;
    }

    @Override
    public CompositeData getSessionCount() {
        return asCompositeData(SESSION_COUNT);
    }

    @Override
    public CompositeData getSessionLogin() {
        return asCompositeData(SESSION_LOGIN_COUNTER);
    }

    @Override
    public CompositeData getSessionReadCount() {
        return asCompositeData(SESSION_READ_COUNTER);
    }

    @Override
    public CompositeData getSessionReadDuration() {
        return asCompositeData(SESSION_READ_DURATION);
    }

    @Override
    public CompositeData getSessionReadAverage() {
        return asCompositeData(SESSION_READ_AVERAGE);
    }

    @Override
    public CompositeData getSessionWriteCount() {
        return asCompositeData(SESSION_WRITE_COUNTER);
    }

    @Override
    public CompositeData getSessionWriteDuration() {
        return asCompositeData(SESSION_WRITE_DURATION);
    }

    @Override
    public CompositeData getSessionWriteAverage() {
        return asCompositeData(SESSION_WRITE_AVERAGE);
    }

    @Override
    public CompositeData getQueryCount() {
        return asCompositeData(QUERY_COUNT);
    }

    @Override
    public CompositeData getQueryDuration() {
        return asCompositeData(QUERY_DURATION);
    }

    @Override
    public CompositeData getQueryAverage() {
        return asCompositeData(QUERY_AVERAGE);
    }

    @Override
    public CompositeData getObservationEventCount() {
        return asCompositeData(OBSERVATION_EVENT_COUNTER);
    }

    @Override
    public CompositeData getObservationEventDuration() {
        return asCompositeData(OBSERVATION_EVENT_DURATION);
    }

    @Override
    public CompositeData getObservationEventAverage() {
        return asCompositeData(OBSERVATION_EVENT_AVERAGE);
    }

    @Override
    public CompositeData getObservationQueueMaxLength() {
        return TimeSeriesStatsUtil
            .asCompositeData(maxQueueLength, "maximal length of observation queue");
    }

    private TimeSeries getTimeSeries(Type type) {
        return repoStats.getTimeSeries(type);
    }

    private CompositeData asCompositeData(Type type) {
        return TimeSeriesStatsUtil.asCompositeData(getTimeSeries(type), type.name());
    }

}
