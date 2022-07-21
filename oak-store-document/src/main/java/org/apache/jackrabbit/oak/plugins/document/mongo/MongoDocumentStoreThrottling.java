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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreThrottling;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.SECONDS;

public class MongoDocumentStoreThrottling implements DocumentStoreThrottling {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentStoreThrottling.class);
    static final String TS_TIME = "ts";
    private static final String NATURAL = "$natural";
    private static final String MAX_SIZE = "maxSize";
    private static final String OPLOG_RS = "oplog.rs";
    public static final String SIZE = "size";
    private final ScheduledExecutorService throttlingExecutor;
    private final AtomicDouble oplogWindow;
    private final MongoDatabase localDb;

    public MongoDocumentStoreThrottling(final @NotNull MongoDatabase localDb, final @NotNull MongoThrottlingMetrics mongoThrottlingMetrics) {
        this.throttlingExecutor = Executors.newSingleThreadScheduledExecutor();
        this.oplogWindow = mongoThrottlingMetrics.oplogWindow;
        this.localDb = localDb;
    }

    public void updateMetrics() {
        throttlingExecutor.scheduleAtFixedRate(() -> {
            Document document = localDb.runCommand(new Document("collStats", OPLOG_RS));
            if (!document.containsKey(MAX_SIZE) || !document.containsKey(SIZE)) {
                LOG.warn("Could not get stats for local.{}  collection. collstats returned: {}.", OPLOG_RS, document);
            } else {
                int maxSize = document.getInteger(MAX_SIZE);
                double maxSizeGb = (double) maxSize / (1024 * 1024 * 1024);
                int usedSize = document.getInteger(SIZE);
                double usedSizeGb = Math.ceil(((double) usedSize / (1024 * 1024 * 1024)) * 1000000) / 1000000;
                MongoCollection<Document> localDbCollection = localDb.getCollection(OPLOG_RS);
                Document first = localDbCollection.find().sort(new Document(NATURAL, 1)).limit(1).first();
                Document last = localDbCollection.find().sort(new Document(NATURAL, -1)).limit(1).first();

                if (Objects.isNull(first) || Objects.isNull(last)) {
                    LOG.warn("Objects not found in local.oplog.rs -- is this a new and empty db instance?");
                } else {
                    if (!first.containsKey(TS_TIME) || !last.containsKey(TS_TIME)) {
                        LOG.warn("ts element not found in oplog objects");
                    } else {
                        oplogWindow.set(updateOplogWindow(maxSizeGb, usedSizeGb, first, last));
                    }
                }
            }
        }, 10, 30, SECONDS);
    }

    // helper methods
    @VisibleForTesting
    double updateOplogWindow(final double maxSize, final double usedSize, final @NotNull Document first,
                                   final @NotNull Document last) {
        final BsonTimestamp startTime = first.get(TS_TIME, BsonTimestamp.class);
        final BsonTimestamp lastTime = last.get(TS_TIME, BsonTimestamp.class);
        long timeDiffSec = Math.abs(lastTime.getTime() - startTime.getTime());
        double timeDiffHr = Math.ceil(((double)timeDiffSec/(60*60)) * 100000)/100000;
        double currentOplogHourRate = usedSize /timeDiffHr;
        double timeLeft = maxSize /currentOplogHourRate;
        LOG.info("Replication info: Oplog Max Size {} Gb, Used Oplog Size {} Gb, First Oplog " +
                "Entry {}, Last Oplog Entry {}, Oplog Entries Time Diff {} sec, Oplog-Gb/hour " +
                "rate {}, time left {}", maxSize, usedSize, startTime.getTime(), lastTime.getTime(),
                timeDiffSec, currentOplogHourRate, timeLeft);
        return timeLeft;
    }
}
