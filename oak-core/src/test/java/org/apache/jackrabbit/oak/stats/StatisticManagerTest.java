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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.jackrabbit.api.jmx.QueryStatManagerMBean;
import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.oak.api.jmx.RepositoryStatsMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class StatisticManagerTest {
    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    @Test
    public void defaultSetup() throws Exception {
        Whiteboard wb = new DefaultWhiteboard();
        StatisticManager mgr = new StatisticManager(wb, executorService);

        MeterStats meterStats = mgr.getMeter(RepositoryStatistics.Type.QUERY_COUNT);
        meterStats.mark(5);

        assertNotNull(WhiteboardUtils.getServices(wb, RepositoryStatsMBean.class));
        assertNotNull(WhiteboardUtils.getServices(wb, QueryStatManagerMBean.class));
    }

    @Test
    public void setupWithCustom() throws Exception {
        Whiteboard wb = new DefaultWhiteboard();
        wb.register(StatisticsProvider.class, StatisticsProvider.NOOP, null);
        StatisticManager mgr = new StatisticManager(wb, executorService);

        MeterStats meterStats = mgr.getMeter(RepositoryStatistics.Type.QUERY_COUNT);
        meterStats.mark(5);

        //TODO Not easy to do any asserts on call. Need to figure out a way
    }

    @After
    public void cleanup() {
        executorService.shutdownNow();
    }
}
