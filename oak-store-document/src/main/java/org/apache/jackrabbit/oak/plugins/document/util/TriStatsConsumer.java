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

import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.TimerStats;

import java.util.List;

/**
 * {@link FunctionalInterface} to consume Metric Stats for create/upsert operation
 * @param <T> the type of the first argument to the consumer
 * @param <U> the type of the second argument to the consumer
 * @param <V> the type of the third argument to the consumer
 */
public interface TriStatsConsumer {

    /**
     * To consume stats for given operation
     *
     * @param meterStats1 Instance of {@link MeterStats}, to collect occurrence of operation
     * @param meterStats2 Instance of {@link MeterStats}, to collect occurrence of operation
     * @param timerStats Instance of {@link TimerStats}, to record operation duration
     * @param ids Ids of nodes updated/created during operation
     * @param tTN time taken to perform the operation (in nanos)
     */
    void accept(final MeterStats meterStats1, final MeterStats meterStats2, final TimerStats timerStats,
                final List<String> ids, final long tTN);
}
