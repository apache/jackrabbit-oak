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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.apache.jackrabbit.guava.common.util.concurrent.AtomicDouble;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.jackrabbit.guava.common.math.DoubleMath.fuzzyEquals;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Math.abs;
import static java.lang.Math.ceil;
import static java.util.Objects.isNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Mongo Document Store throttling metric updater.
 *
 * This class fetches and updates the mongo oplog window
 */
public class MongoDocumentStoreThrottlingMetricsUpdater implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentStoreThrottlingMetricsUpdater.class);
    static final String TS_TIME = "ts";
    private static final String NATURAL = "$natural";
    private static final String MAX_SIZE = "maxSize";
    private static final String OPLOG_RS = "oplog.rs";
    public static final String SIZE = "size";
    private final ScheduledExecutorService throttlingMetricsExecutor;
    private final AtomicDouble oplogWindow;
    private final MongoDatabase localDb;

    public MongoDocumentStoreThrottlingMetricsUpdater(final @NotNull MongoDatabase localDb, final @NotNull AtomicDouble oplogWindow) {
        this.throttlingMetricsExecutor = newSingleThreadScheduledExecutor();
        this.oplogWindow = oplogWindow;
        this.localDb = localDb;
    }

    public void scheduleUpdateMetrics() {
        throttlingMetricsExecutor.scheduleAtFixedRate(() -> {
            Document document = localDb.runCommand(new Document("collStats", OPLOG_RS));
            if (!document.containsKey(MAX_SIZE) || !document.containsKey(SIZE)) {
                LOG.warn("Could not get stats for local.{}  collection. collstats returned: {}.", OPLOG_RS, document);
                oplogWindow.set(MAX_VALUE);
            } else {
                int maxSize = document.getInteger(MAX_SIZE);
                double maxSizeGb = (double) maxSize / (1024 * 1024 * 1024);
                int usedSize = document.getInteger(SIZE);
                double usedSizeGb = ceil(((double) usedSize / (1024 * 1024 * 1024)) * 1000000) / 1000000;
                MongoCollection<Document> localDbCollection = localDb.getCollection(OPLOG_RS);
                Document first = localDbCollection.find().sort(new Document(NATURAL, 1)).limit(1).first();
                Document last = localDbCollection.find().sort(new Document(NATURAL, -1)).limit(1).first();

                if (isNull(first) || isNull(last)) {
                    LOG.warn("Objects not found in local.oplog.rs -- is this a new and empty db instance?");
                    oplogWindow.set(MAX_VALUE);
                } else {
                    if (!first.containsKey(TS_TIME) || !last.containsKey(TS_TIME)) {
                        LOG.warn("ts element not found in oplog objects");
                        oplogWindow.set(MAX_VALUE);
                    } else {
                        oplogWindow.set(updateOplogWindow(maxSizeGb, usedSizeGb, first, last));
                    }
                }
            }
        }, 10, 30, SECONDS);
    }

    // helper methods, visible for testing
    static double updateOplogWindow(final double maxSize, final double usedSize, final @NotNull Document first,
                                   final @NotNull Document last) {
        final BsonTimestamp startTime = first.get(TS_TIME, BsonTimestamp.class);
        final BsonTimestamp lastTime = last.get(TS_TIME, BsonTimestamp.class);

        if (Objects.equals(startTime, lastTime) || fuzzyEquals(usedSize, 0, 0.00001)) {
            return MAX_VALUE;
        }
        long timeDiffSec = abs(lastTime.getTime() - startTime.getTime());
        double timeDiffHr = ceil(((double)timeDiffSec/(60*60)) * 100000)/100000;
        double currentOplogHourRate = usedSize / timeDiffHr;
        double timeLeft = maxSize / currentOplogHourRate;
        LOG.info("Replication info: Oplog Max Size {} Gb, Used Oplog Size {} Gb, First Oplog " +
                "Entry {}, Last Oplog Entry {}, Oplog Entries Time Diff {} sec, Oplog-Gb/hour " +
                "rate {}, time left {}", maxSize, usedSize, startTime.getTime(), lastTime.getTime(),
                timeDiffSec, currentOplogHourRate, timeLeft);
        return timeLeft;
    }

    @Override
    public void close() throws IOException {
        new ExecutorCloser(this.throttlingMetricsExecutor).close();
    }
}
