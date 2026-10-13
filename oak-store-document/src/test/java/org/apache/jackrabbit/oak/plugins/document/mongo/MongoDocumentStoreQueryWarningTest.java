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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.commons.time.Stopwatch;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.event.Level;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for MongoDB query warning threshold logging functionality.
 */
public class MongoDocumentStoreQueryWarningTest {

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    private LogCustomizer logCustomizer;
    private MongoConnection mongoConnection;
    private TestStore store;

    @BeforeClass
    public static void checkMongoDbAvailable() {
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

    @Before
    public void setUp() {
        mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDatabase());
        logCustomizer = LogCustomizer
                .forLogger(MongoDocumentStore.class)
                .enable(Level.WARN)
                .contains("MongoDB query exceeded threshold")
                .create();
    }

    @After
    public void tearDown() {
        if (logCustomizer != null) {
            logCustomizer.finished();
        }
        if (store != null) {
            store.dispose();
            store = null;
        }
    }

    @Test
    public void queryWarningDisabledByDefault() {
        // Create store with default threshold (0 = disabled)
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        store = new TestStore(mongoConnection.getMongoClient(), 
                mongoConnection.getDatabase(), builder, 0);

        logCustomizer.starting();
        
        // Perform query - even with mocked timing, no warning when threshold is 0
        store.find(Collection.NODES, "non-existing-key");
        
        // No warning should be logged when threshold is 0
        assertEquals("No warnings should be logged when threshold is 0",
                0, logCustomizer.getLogs().size());
    }

    @Test
    public void queryWarningNotLoggedWhenBelowThreshold() {
        // Create store with threshold of 20000 milliseconds (20 seconds)
        // Mock timing returns 1000ms (below threshold)
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder()
                .setMongoQueryWarningThresholdMillis(20000);
        store = new TestStore(mongoConnection.getMongoClient(), 
                mongoConnection.getDatabase(), builder, 1000);

        logCustomizer.starting();
        
        // Perform query with mocked 1000ms duration (below 20000ms threshold)
        store.find(Collection.NODES, "testnode");
        
        // No warning should be logged for queries completing below threshold
        assertEquals("No warning should be logged for queries completing below threshold", 
                0, logCustomizer.getLogs().size());
    }

    @Test
    public void queryWarningLoggedWhenExceedsThreshold() {
        // Create store with 5000ms threshold, mock timing returns 10000ms (exceeds threshold)
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder()
                .setMongoQueryWarningThresholdMillis(5000);
        store = new TestStore(mongoConnection.getMongoClient(), 
                mongoConnection.getDatabase(), builder, 10000);

        logCustomizer.starting();
        
        // Perform query with mocked 10000ms duration (exceeds 5000ms threshold)
        store.find(Collection.NODES, "testnode");
        
        // Warning should be logged
        assertTrue("Warning should be logged when query exceeds threshold", 
                logCustomizer.getLogs().size() > 0);
        
        // Verify warning message contains expected information
        String logMessage = logCustomizer.getLogs().get(0);
        assertTrue("Log message should contain query description", 
                logMessage.contains("findUncached"));
        assertTrue("Log message should contain duration",
                logMessage.contains("took"));
        assertTrue("Log message should contain 'ms'",
                logMessage.contains("ms"));
        assertTrue("Log message should contain collection name",
                logMessage.contains("nodes"));
    }

    @Test
    public void warningIncludesQueryDetails() {
        // Test that query details are included in warning message
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder()
                .setMongoQueryWarningThresholdMillis(5000);
        store = new TestStore(mongoConnection.getMongoClient(), 
                mongoConnection.getDatabase(), builder, 10000);

        logCustomizer.starting();
        
        // Test with a specific key
        store.find(Collection.NODES, "testnode");
        
        // Verify warning was logged with details
        assertTrue("At least one warning should be logged", 
                logCustomizer.getLogs().size() > 0);
        
        String logMessage = logCustomizer.getLogs().get(0);
        assertTrue("Log message should contain query details",
                logMessage.contains("testnode") || logMessage.contains("findUncached"));
    }

    /**
     * Test subclass that overrides startWatch() to return a Stopwatch with a custom ticker.
     * This allows testing the threshold logic without relying on actual query performance.
     */
    private static class TestStore extends MongoDocumentStore {
        private final long mockElapsedMillis;

        public TestStore(MongoClient client, MongoDatabase db, 
                        MongoDocumentNodeStoreBuilder builder,
                        long mockElapsedMillis) {
            super(client, db, builder);
            this.mockElapsedMillis = mockElapsedMillis;
        }

        @Override
        protected Stopwatch startWatch() {
            // Create a Stopwatch with a custom ticker that simulates the specified elapsed time
            // The ticker simulates time progression: first call returns 0, subsequent calls add the mock duration
            long startNanos = System.nanoTime();
            long elapsedNanos = TimeUnit.MILLISECONDS.toNanos(mockElapsedMillis);
            long[] callCount = {0};
            
            return Stopwatch.createStarted(() -> {
                long count = callCount[0]++;
                // Call 0 & 1: Constructor and start() - return start time
                // Call 2+: elapsed() calls - return start time + elapsed duration
                return count < 2 ? startNanos : startNanos + elapsedNanos;
            });
        }
    }
}
