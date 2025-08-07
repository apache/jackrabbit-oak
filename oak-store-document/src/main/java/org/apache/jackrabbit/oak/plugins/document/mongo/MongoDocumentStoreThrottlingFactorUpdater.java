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

import com.mongodb.client.MongoDatabase;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Reads throttling values from the MongoDB settings collection.
 * <p>
 * This class provides methods to fetch the throttling factor and related settings
 * from the MongoDB database for use in throttling logic.
 */
public class MongoDocumentStoreThrottlingFactorUpdater implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentStoreThrottlingFactorUpdater.class);
    private static final String SETTINGS = "settings";
    private static final String ENABLE = "enable";
    private static final String FACTOR = "factor";
    private static final String TS_TIME = "ts";
    public static final String SIZE = "size";
    private final ScheduledExecutorService throttlingFactorExecutor;
    private final AtomicReference<Integer> factorRef;
    private final MongoDatabase localDb;
    private final int period;

    public MongoDocumentStoreThrottlingFactorUpdater(final @NotNull MongoDatabase localDb,
                                                     final @NotNull AtomicReference<Integer> factor,
                                                     int period) {
        this.throttlingFactorExecutor = Executors.newSingleThreadScheduledExecutor();
        this.factorRef = factor;
        this.localDb = localDb;
        this.period = period;
    }

    public void scheduleFactorUpdates() {
        throttlingFactorExecutor.scheduleAtFixedRate(() -> factorRef.set(updateFactor()), 10, period, SECONDS);
    }

    // visible for testing only
    public int updateFactor() {
        final Document document = localDb.runCommand(new Document("throttling", SETTINGS));

        if (!document.containsKey(ENABLE) || !document.containsKey(FACTOR) || !document.containsKey(TS_TIME)) {
            LOG.warn("Could not get values for settings.{} collection. Document returned: {}. Setting throttling factor to 0", "throttling", document);
            return 0;
        }
        if (!document.getBoolean(ENABLE)) {
            LOG.debug("Throttling has been disabled. Setting throttling factor to 0.");
            return 0;
        }

        long ts = document.getLong(TS_TIME);
        long now = System.currentTimeMillis();
        if (now - ts > 3600000) { // 1 hour in ms
            LOG.warn("Throttling timestamp is older than 1 hour. Setting throttling factor to 0");
            return 0;
        }

        int factor = document.getInteger(FACTOR, 0);
        if (factor <= 0) {
            LOG.warn("Throttling factor is less than or equal to 0. Setting throttling factor to 0");
            return 0;
        }
        return factor;
    }


    @Override
    public void close() throws IOException {
        new ExecutorCloser(this.throttlingFactorExecutor).close();
    }
}
