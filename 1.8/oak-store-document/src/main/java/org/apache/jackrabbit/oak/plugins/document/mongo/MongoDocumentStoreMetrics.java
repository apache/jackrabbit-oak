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
 */
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoException;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.stats.StatsOptions.METRICS_ONLY;

/**
 * Implementation specific metrics exposed by the {@link MongoDocumentStore}.
 */
public final class MongoDocumentStoreMetrics implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentStoreMetrics.class);

    private static final ImmutableList<Collection<? extends Document>> COLLECTIONS = ImmutableList.of(
            Collection.NODES, Collection.JOURNAL, Collection.CLUSTER_NODES, Collection.SETTINGS, Collection.BLOBS
    );

    private final DB db;

    private final StatisticsProvider statsProvider;

    public MongoDocumentStoreMetrics(MongoDocumentStore store,
                                     StatisticsProvider statsProvider) {
        this.db = store.getDBCollection(Collection.NODES).getDB();
        this.statsProvider = statsProvider;
    }

    //-----------------------< Runnable >---------------------------------------

    @Override
    public void run() {
        updateCounters();
    }

    //-----------------------< internal >---------------------------------------

    private void updateCounters() {
        LOG.debug("Updating counters");
        try {
            Set<String> collectionNames = db.getCollectionNames();
            for (Collection<? extends Document> c : COLLECTIONS) {
                if (!collectionNames.contains(c.toString())) {
                    LOG.debug("Collection {} does not exist", c);
                    continue;
                }
                CollectionStats stats = getStats(c);
                updateCounter(getCounter(c, "count"), stats.count);
                updateCounter(getCounter(c, "size"), stats.size);
                updateCounter(getCounter(c, "storageSize"), stats.storageSize);
                updateCounter(getCounter(c, "totalIndexSize"), stats.totalIndexSize);
            }
        } catch (MongoException e) {
            LOG.warn("Updating counters failed: {}", e.toString());
        }
    }

    private void updateCounter(CounterStats counter, long value) {
        counter.inc(value - counter.getCount());
    }

    private CollectionStats getStats(Collection<? extends Document> c)
            throws MongoException {
        CollectionStats stats = new CollectionStats();
        BasicDBObject result = db.getCollection(c.toString()).getStats();
        stats.count = result.getLong("count", 0);
        stats.size = result.getLong("size", 0);
        stats.storageSize = result.getLong("storageSize", 0);
        stats.totalIndexSize = result.getLong("totalIndexSize", 0);
        return stats;
    }

    private CounterStats getCounter(Collection<? extends Document> c,
                                    String name) {
        String counterName = "MongoDB." + c.toString() + "." + name;
        return statsProvider.getCounterStats(counterName, METRICS_ONLY);
    }

    private static final class CollectionStats {
        long count;
        long size;
        long storageSize;
        long totalIndexSize;
    }
}
