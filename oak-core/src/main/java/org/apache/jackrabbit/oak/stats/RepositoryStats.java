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

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.api.jmx.RepositoryStatsMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepositoryStats implements RepositoryStatsMBean {
    private static final Logger LOG = LoggerFactory.getLogger(RepositoryStats.class);

    private final RepositoryStatistics repoStats;

    public RepositoryStats(RepositoryStatistics repoStats) {
        this.repoStats = repoStats;
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
        return asCompositeData(Type.QUERY_AVERAGE);
    }

    public static final String[] ITEM_NAMES = new String[] {
            "per second", "per minute", "per hour", "per week"};

    private CompositeData asCompositeData(Type type) {
        try {
            TimeSeries timeSeries = repoStats.getTimeSeries(type);
            long[][] values = new long[][] {
                timeSeries.getValuePerSecond(),
                timeSeries.getValuePerMinute(),
                timeSeries.getValuePerHour(),
                timeSeries.getValuePerWeek()};
            return new CompositeDataSupport(getCompositeType(type), ITEM_NAMES, values);
        } catch (Exception e) {
            LOG.error("Error creating CompositeData instance from TimeSeries", e);
            return null;
        }
    }

    private static CompositeType getCompositeType(Type type) throws OpenDataException {
        ArrayType<int[]> longArrayType = new ArrayType<int[]>(SimpleType.LONG, true);
        OpenType<?>[] itemTypes = new OpenType[] {
                longArrayType, longArrayType, longArrayType, longArrayType};
        String name = type.toString();
        return new CompositeType(name, name + " time series", ITEM_NAMES, ITEM_NAMES, itemTypes);
    }

}
