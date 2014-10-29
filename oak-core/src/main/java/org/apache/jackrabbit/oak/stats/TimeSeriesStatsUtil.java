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

import org.apache.jackrabbit.api.stats.TimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

/**
 * Utility class for retrieving {@link javax.management.openmbean.CompositeData} for
 * {@link org.apache.jackrabbit.api.stats.TimeSeries}.
 */
public class TimeSeriesStatsUtil {
    public static final String[] ITEM_NAMES = new String[] {"per second", "per minute", "per hour", "per week"};

    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesStatsUtil.class);

    public static CompositeData asCompositeData(TimeSeries timeSeries, String name) {
        try {
            long[][] values = new long[][] {timeSeries.getValuePerSecond(), timeSeries.getValuePerMinute(),
                timeSeries.getValuePerHour(), timeSeries.getValuePerWeek()};
            return new CompositeDataSupport(getCompositeType(name), ITEM_NAMES, values);
        } catch (Exception e) {
            LOG.error("Error creating CompositeData instance from TimeSeries", e);
            return null;
        }
    }

    static CompositeType getCompositeType(String name) throws OpenDataException {
        ArrayType<int[]> longArrayType = new ArrayType<int[]>(SimpleType.LONG, true);
        OpenType<?>[] itemTypes = new OpenType[] {longArrayType, longArrayType, longArrayType, longArrayType};
        return new CompositeType(name, name + " time series", ITEM_NAMES, ITEM_NAMES, itemTypes);
    }
}
