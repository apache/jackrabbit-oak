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

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreStatsCollector;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.TimerStats;

import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Base class to update the metrics for {@link DocumentStoreStatsCollector#doneFindAndModify(long, Collection, String, boolean, boolean, int)} for underlying {@link DocumentStore}
 *
 * <p>Users provide instances of {@link MeterStats}, {@link TimerStats} based on whether throttling is ongoing or not
 */
public final class ModifyMetricUpdater {

    private final MeterStats createNodeUpsertMeter;
    private final TimerStats createNodeUpsertTimer;
    private final MeterStats updateNodeMeter;
    private final TimerStats updateNodeTimer;
    private final MeterStats updateNodeRetryCountMeter;
    private final MeterStats updateNodeFailureMeter;

    public ModifyMetricUpdater(final MeterStats createNodeUpsertMeter,
                               final TimerStats createNodeUpsertTimer,
                               final MeterStats updateNodeMeter,
                               final TimerStats updateNodeTimer,
                               final MeterStats updateNodeRetryCountMeter,
                               final MeterStats updateNodeFailureMeter) {
        this.createNodeUpsertMeter = createNodeUpsertMeter;
        this.createNodeUpsertTimer = createNodeUpsertTimer;
        this.updateNodeMeter = updateNodeMeter;
        this.updateNodeTimer = updateNodeTimer;
        this.updateNodeRetryCountMeter = updateNodeRetryCountMeter;
        this.updateNodeFailureMeter = updateNodeFailureMeter;
    }

    public void update(final Collection<? extends Document> collection, final int retryCount,
                       final long timeTakenNanos, final boolean isSuccess, final boolean  newEntry,
                       final Predicate<Collection<? extends Document>> isNodesCollection,
                       final BiStatsConsumer createBiStatsConsumer,
                       final BiStatsConsumer updateBiStatsConsumer,
                       final ObjIntConsumer<MeterStats> retryNodesConsumer,
                       final Consumer<MeterStats> failureNodesConsumer) {

        requireNonNull(isNodesCollection);
        requireNonNull(createBiStatsConsumer);
        requireNonNull(updateBiStatsConsumer);
        requireNonNull(retryNodesConsumer);
        requireNonNull(failureNodesConsumer);

        if (isNodesCollection.negate().test(collection)) {
            return;
        }

        if (isSuccess) {
            if (newEntry) {
                createBiStatsConsumer.accept(createNodeUpsertMeter, createNodeUpsertTimer, 1, timeTakenNanos);
            } else {
                updateBiStatsConsumer.accept(updateNodeMeter, updateNodeTimer, 1, timeTakenNanos);
            }
            if (retryCount > 0) retryNodesConsumer.accept(updateNodeRetryCountMeter, retryCount);
        } else {
            retryNodesConsumer.accept(updateNodeRetryCountMeter, retryCount);
            failureNodesConsumer.accept(updateNodeFailureMeter);
        }
    }
}
