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
package org.apache.jackrabbit.oak.plugins.document.util;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.junit.After;

import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.ImmutableList.of;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public abstract class BaseUpdaterTest {

     static final String NODES_CREATE_UPSERT_TIMER = "NODES_CREATE_UPSERT_TIMER";
     static final String NODES_UPDATE = "NODES_UPDATE";
     static final String NODES_UPDATE_TIMER = "NODES_UPDATE_TIMER";
     static final String NODES_UPDATE_RETRY_COUNT = "NODES_UPDATE_RETRY_COUNT";
     static final String NODES_UPDATE_FAILURE = "NODES_UPDATE_FAILURE";
     static final String NODES_CREATE_UPSERT_THROTTLING_TIMER = "NODES_CREATE_UPSERT_THROTTLING_TIMER";
     static final String NODES_UPDATE_THROTTLING = "NODES_UPDATE_THROTTLING";
     static final String NODES_UPDATE_THROTTLING_TIMER = "NODES_UPDATE_THROTTLING_TIMER";
     static final String NODES_UPDATE_RETRY_COUNT_THROTTLING = "NODES_UPDATE_RETRY_COUNT_THROTTLING";
     static final String NODES_UPDATE_FAILURE_THROTTLING = "NODES_UPDATE_FAILURE_THROTTLING";
     static final String NODES_CREATE_UPSERT = "NODES_CREATE_UPSERT";
     static final String NODES_CREATE_UPSERT_THROTTLING = "NODES_CREATE_UPSERT_THROTTLING";
     static final String NODES_CREATE_SPLIT = "NODES_CREATE_SPLIT";
     static final String NODES_CREATE_SPLIT_THROTTLING = "NODES_CREATE_SPLIT_THROTTLING";
     static final String NODES_CREATE = "NODES_CREATE";
     static final String NODES_CREATE_TIMER = "NODES_CREATE_TIMER";
     static final String JOURNAL_CREATE = "JOURNAL_CREATE";
     static final String JOURNAL_CREATE_TIMER = "JOURNAL_CREATE_TIMER";
     static final String NODES_CREATE_THROTTLING = "NODES_CREATE_THROTTLING";
     static final String NODES_CREATE_THROTTLING_TIMER = "NODES_CREATE_THROTTLING_TIMER";
     static final String JOURNAL_CREATE_THROTTLING = "JOURNAL_CREATE_THROTTLING";
     static final String JOURNAL_CREATE_THROTTLING_TIMER = "JOURNAL_CREATE_THROTTLING_TIMER";
     static final String NODES_REMOVE = "NODES_REMOVE";
     static final String NODES_REMOVE_TIMER = "NODES_REMOVE_TIMER";
     static final String NODES_REMOVE_THROTTLING = "NODES_REMOVE_THROTTLING";
     static final String NODES_REMOVE_THROTTLING_TIMER = "NODES_REMOVE_THROTTLING_TIMER";
     final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
     final MetricStatisticsProvider provider = new MetricStatisticsProvider(getPlatformMBeanServer(), executor);
     final ImmutableList<String> ids = of("a", "b");

     Meter getMeter(String name) {
        return provider.getRegistry().getMeters().get(name);
    }

     Timer getTimer(String name) {
        return provider.getRegistry().getTimers().get(name);
    }

    @After
    public void shutDown() {
        provider.close();
        new ExecutorCloser(executor).close();
    }
}
