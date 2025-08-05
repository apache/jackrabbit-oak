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

import com.mongodb.MongoClientSettings;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

/**
 * Tests for MongoDB connection pool settings configuration.
 * These are unit tests that verify MongoClientSettings are built correctly.
 */
public class MongoConnectionPoolSettingsTest {

    /**
     * Helper method to create a builder with URI and name set for testing,
     * without actually connecting to MongoDB.
     */
    private MongoDocumentNodeStoreBuilder createTestBuilder() throws Exception {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        
        // Use reflection to set uri and name fields without connecting
        Field uriField = MongoDocumentNodeStoreBuilderBase.class.getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(builder, "mongodb://localhost:27017");
        
        Field nameField = MongoDocumentNodeStoreBuilderBase.class.getDeclaredField("name");
        nameField.setAccessible(true);
        nameField.set(builder, "test");
        
        return builder;
    }

    @Test
    public void testDefaultMongoClientSettings() throws Exception {
        MongoDocumentNodeStoreBuilder builder = createTestBuilder();

        // Test default connection (isLease = false)
        MongoClientSettings settings = builder.buildMongoClientSettings(false);
        assertNotNull(settings);
        
        // Verify default connection pool settings
        assertEquals(DocumentNodeStoreService.DEFAULT_MONGO_MAX_POOL_SIZE, settings.getConnectionPoolSettings().getMaxSize());
        assertEquals(DocumentNodeStoreService.DEFAULT_MONGO_MIN_POOL_SIZE, settings.getConnectionPoolSettings().getMinSize());
        assertEquals(DocumentNodeStoreService.DEFAULT_MONGO_MAX_CONNECTING, settings.getConnectionPoolSettings().getMaxConnecting());
        
        // Verify default socket settings
        assertEquals(DocumentNodeStoreService.DEFAULT_MONGO_READ_TIMEOUT_MILLIS, settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
        assertEquals(DocumentNodeStoreService.DEFAULT_MONGO_CONNECT_TIMEOUT_MILLIS, settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS));
        
        // Verify default server settings
        assertEquals(DocumentNodeStoreService.DEFAULT_MONGO_HEARTBEAT_FREQUENCY_MILLIS, settings.getServerSettings().getHeartbeatFrequency(TimeUnit.MILLISECONDS));
        assertEquals(DocumentNodeStoreService.DEFAULT_MONGO_MIN_HEARTBEAT_FREQUENCY_MILLIS, settings.getServerSettings().getMinHeartbeatFrequency(TimeUnit.MILLISECONDS));
        
        // Verify default cluster settings
        assertEquals(DocumentNodeStoreService.DEFAULT_MONGO_SERVER_SELECTION_TIMEOUT_MILLIS, settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testCustomMongoClientSettings() throws Exception {
        MongoDocumentNodeStoreBuilder builder = createTestBuilder()
                // Set custom connection pool settings
                .setMongoMaxPoolSize(57)
                .setMongoMinPoolSize(13)
                .setMongoMaxConnecting(7)
                .setMongoMaxIdleTimeMillis(61113)
                .setMongoMaxLifeTimeMillis(303030)
                .setMongoWaitQueueTimeoutMillis(41091)
                // Set custom socket settings
                .setMongoConnectTimeoutMillis(5123)
                .setMongoReadTimeoutMillis(29011)
                // Set custom server settings
                .setMongoHeartbeatFrequencyMillis(15013)
                .setMongoMinHeartbeatFrequencyMillis(1009)
                // Set custom cluster settings
                .setMongoServerSelectionTimeoutMillis(10999);

        // Test default connection (isLease = false)
        MongoClientSettings settings = builder.buildMongoClientSettings(false);
        
        // Verify custom connection pool settings
        assertEquals(57, settings.getConnectionPoolSettings().getMaxSize());
        assertEquals(13, settings.getConnectionPoolSettings().getMinSize());
        assertEquals(7, settings.getConnectionPoolSettings().getMaxConnecting());
        assertEquals(61113, settings.getConnectionPoolSettings().getMaxConnectionIdleTime(TimeUnit.MILLISECONDS));
        assertEquals(303030, settings.getConnectionPoolSettings().getMaxConnectionLifeTime(TimeUnit.MILLISECONDS));
        assertEquals(41091, settings.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS));
        
        // Verify custom socket settings
        assertEquals(5123, settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS));
        assertEquals(29011, settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
        
        // Verify custom server settings
        assertEquals(15013, settings.getServerSettings().getHeartbeatFrequency(TimeUnit.MILLISECONDS));
        assertEquals(1009, settings.getServerSettings().getMinHeartbeatFrequency(TimeUnit.MILLISECONDS));
        
        // Verify custom cluster settings
        assertEquals(10999, settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testLeaseConnectionSocketTimeout() throws Exception {
        MongoDocumentNodeStoreBuilder builder = createTestBuilder()
                .setMongoReadTimeoutMillis(55001)  // Default connection timeout
                .setLeaseSocketTimeout(33002); // Lease connection timeout

        // Test default connection pool (isLease = false) - should use readTimeout
        MongoClientSettings mainSettings = builder.buildMongoClientSettings(false);
        assertEquals(55001, mainSettings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));

        // Test lease connection (isLease = true) - should use leaseSocketTimeout
        MongoClientSettings leaseSettings = builder.buildMongoClientSettings(true);
        assertEquals(33002, leaseSettings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testZeroTimeoutValues() throws Exception {
        MongoDocumentNodeStoreBuilder builder = createTestBuilder()
                .setMongoMaxIdleTimeMillis(0)           // Disabled
                .setMongoMaxLifeTimeMillis(0)           // Disabled
                .setMongoConnectTimeoutMillis(0)        // Disabled
                .setMongoReadTimeoutMillis(0)           // Disabled
                .setMongoServerSelectionTimeoutMillis(0); // Disabled

        MongoClientSettings settings = builder.buildMongoClientSettings(false);
        
        // Verify zero values are preserved (unlimited/disabled timeouts)
        assertEquals(0, settings.getConnectionPoolSettings().getMaxConnectionIdleTime(TimeUnit.MILLISECONDS));
        assertEquals(0, settings.getConnectionPoolSettings().getMaxConnectionLifeTime(TimeUnit.MILLISECONDS));
        assertEquals(0, settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS));
        assertEquals(0, settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
        assertEquals(0, settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testMainConnectionWithoutReadTimeout() throws Exception {
        MongoDocumentNodeStoreBuilder builder = createTestBuilder();
        // Don't set readTimeout (should default to 0)

        MongoClientSettings settings = builder.buildMongoClientSettings(false);
        
        // Main connection without explicit readTimeout should use 0 (unlimited)
        assertEquals(0, settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testLeaseConnectionWithDefaultTimeout() throws Exception {
        MongoDocumentNodeStoreBuilder builder = createTestBuilder();

        MongoClientSettings settings = builder.buildMongoClientSettings(true);
        
        // Lease connection should use default lease socket timeout 
        assertEquals(DocumentNodeStoreService.DEFAULT_MONGO_LEASE_SO_TIMEOUT_MILLIS, settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testBuilderSettersReturnCorrectValues() throws Exception {
        MongoDocumentNodeStoreBuilder builder = createTestBuilder()
                .setMongoMaxPoolSize(75)
                .setMongoMinPoolSize(10)
                .setMongoConnectTimeoutMillis(15000);

        // Test that setters return the builder itself
        assertSame(builder, builder.setMongoMaxPoolSize(75));
        
        // Test that settings are applied correctly
        MongoClientSettings settings = builder.buildMongoClientSettings(false);
        assertEquals(75, settings.getConnectionPoolSettings().getMaxSize());
        assertEquals(10, settings.getConnectionPoolSettings().getMinSize());
        assertEquals(15000, settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS));
    }
} 