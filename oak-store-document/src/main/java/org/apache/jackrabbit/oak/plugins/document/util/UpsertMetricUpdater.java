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

import java.util.List;
import java.util.function.BiPredicate;

import static java.util.Objects.requireNonNull;

/**
 * Base class to update the metrics for {@link DocumentStoreStatsCollector#doneCreateOrUpdate(long, Collection, List)} for underlying {@link DocumentStore}
 *
 * <p>Users provide instances of {@link MeterStats}, {@link TimerStats} based on whether throttling is ongoing or not
 */
public final class UpsertMetricUpdater {

    private final MeterStats createNodeUpsertMeter;
    private final MeterStats createSplitNodeMeter;
    private final TimerStats createNodeUpsertTimer;

    public UpsertMetricUpdater(final MeterStats createNodeUpsertMeter,
                               final MeterStats createSplitNodeMeter,
                               final TimerStats createNodeUpsertTimer) {
        this.createNodeUpsertMeter = createNodeUpsertMeter;
        this.createSplitNodeMeter = createSplitNodeMeter;
        this.createNodeUpsertTimer = createNodeUpsertTimer;
    }

    public void update(final Collection<? extends Document> collection, final long timeTakenNanos,
                       final List<String> ids, final BiPredicate<Collection<? extends Document>, Integer> isNodesCollectionUpdated,
                       final TriStatsConsumer upsertStatsConsumer) {

        requireNonNull(isNodesCollectionUpdated);
        requireNonNull(upsertStatsConsumer);

        if (isNodesCollectionUpdated.negate().test(collection, ids.size())) {
            return;
        }
        upsertStatsConsumer.accept(createNodeUpsertMeter, createSplitNodeMeter, createNodeUpsertTimer, ids, timeTakenNanos);
    }
}
