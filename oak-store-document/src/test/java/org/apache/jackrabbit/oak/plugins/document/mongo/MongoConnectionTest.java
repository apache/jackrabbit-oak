/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;

import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MongoConnectionTest {

    private boolean DEBUG = false;

    @Test
    public void hasWriteConcern() throws Exception {
        assertFalse(MongoConnection.hasWriteConcern("mongodb://localhost:27017/foo"));
        assertTrue(MongoConnection.hasWriteConcern("mongodb://localhost:27017/foo?w=1"));
    }

    @Test
    public void hasReadConcern() throws Exception {
        assertFalse(MongoConnection.hasReadConcern("mongodb://localhost:27017/foo"));
        assertTrue(MongoConnection.hasReadConcern("mongodb://localhost:27017/foo?readconcernlevel=majority"));
    }

    @Test
    public void testHasWriteConcern_withExplicitWParam() {
        String uriWithW = "mongodb://localhost:27017/?w=majority";
        assertTrue(MongoConnection.hasWriteConcern(uriWithW));
    }

    @Test
    public void testHasWriteConcern_withoutWParam() {
        String uriWithoutW = "mongodb://localhost:27017";
        assertFalse(MongoConnection.hasWriteConcern(uriWithoutW));
    }

    @Test
    public void testHasWriteConcern_withUnknownParam() {
        String uriWithOtherParams = "mongodb://localhost:27017/?retryWrites=true";
        assertFalse(MongoConnection.hasWriteConcern(uriWithOtherParams));
    }

    @Test
    public void testHasWriteConcern_withWEqual1() {
        String uriWithW1 = "mongodb://localhost:27017/?w=1";
        assertTrue(MongoConnection.hasWriteConcern(uriWithW1));
    }

    @Test
    public void sufficientWriteConcern() throws Exception {
        sufficientWriteConcernReplicaSet(WriteConcern.ACKNOWLEDGED, false);
        sufficientWriteConcernReplicaSet(WriteConcern.JOURNALED, false);
        sufficientWriteConcernReplicaSet(WriteConcern.MAJORITY, true);
        sufficientWriteConcernReplicaSet(WriteConcern.W2, true);
        sufficientWriteConcernReplicaSet(WriteConcern.UNACKNOWLEDGED, false);

        sufficientWriteConcernSingleNode(WriteConcern.ACKNOWLEDGED, true);
        sufficientWriteConcernSingleNode(WriteConcern.JOURNALED, true);
        sufficientWriteConcernSingleNode(WriteConcern.MAJORITY, true);
        sufficientWriteConcernReplicaSet(WriteConcern.W2, true);
        sufficientWriteConcernSingleNode(WriteConcern.UNACKNOWLEDGED, false);
    }

    @Test
    public void sufficientReadConcern() throws Exception {
        sufficientReadConcernReplicaSet(ReadConcern.DEFAULT, false);
        sufficientReadConcernReplicaSet(ReadConcern.LOCAL, false);
        sufficientReadConcernReplicaSet(ReadConcern.MAJORITY, true);

        sufficientReadConcernSingleNode(ReadConcern.DEFAULT, true);
        sufficientReadConcernSingleNode(ReadConcern.LOCAL, true);
        sufficientReadConcernSingleNode(ReadConcern.MAJORITY, true);
    }

    private void sufficientWriteConcernReplicaSet(WriteConcern w,
                                                  boolean sufficient) {
        sufficientWriteConcern(w, true, sufficient);
    }

    private void sufficientWriteConcernSingleNode(WriteConcern w,
                                                  boolean sufficient) {
        sufficientWriteConcern(w, false, sufficient);
    }

    private void sufficientWriteConcern(WriteConcern w,
                                        boolean replicaSet,
                                        boolean sufficient) {
        MongoClient mongo = mockMongoClient(replicaSet);
        assertEquals(sufficient, MongoConnection.isSufficientWriteConcern(mongo, w));
    }

    private void sufficientReadConcernReplicaSet(ReadConcern r,
                                                 boolean sufficient) {
        sufficientReadConcern(r, true, sufficient);
    }

    private void sufficientReadConcernSingleNode(ReadConcern r,
                                                 boolean sufficient) {
        sufficientReadConcern(r, false, sufficient);
    }
    private void sufficientReadConcern(ReadConcern r,
                                       boolean replicaSet,
                                       boolean sufficient) {
        MongoClient mongo = mockMongoClient(replicaSet);
        assertEquals(sufficient, MongoConnection.isSufficientReadConcern(mongo, r));
    }

    private MongoClient mockMongoClient(boolean replicaSet) {
        ClusterDescription description = mock(ClusterDescription.class);
        if (replicaSet) {
            when(description.getType()).thenReturn(ClusterType.REPLICA_SET);
        } else {
            when(description.getType()).thenReturn(ClusterType.STANDALONE);
        }
        MongoClient client = mock(MongoClient.class);
        when(client.getClusterDescription()).thenReturn(description);
        return client;
    }

    @Test
    public void testMongoConnectionGetDefaultBuilderUsesOakDefaults() {
        String testUri = "mongodb://localhost:27017/testdb";
        ConnectionString mongoURI = new ConnectionString(testUri);

        MongoClientSettings settings = MongoConnection.getDefaultBuilder()
                .applyConnectionString(mongoURI)
                .build();

        ConnectionPoolSettings poolSettings = settings.getConnectionPoolSettings();
        ServerSettings serverSettings = settings.getServerSettings();

        // Verify that the settings match Oak MongoConnection.DEFAULT_MAX_WAIT_TIME
        assertEquals(60000, (int) poolSettings.getMaxWaitTime(TimeUnit.MILLISECONDS));
        // Verify that the settings match Oak MongoConnection.DEFAULT_HEARTBEAT_FREQUENCY
        assertEquals(5000, (int) serverSettings.getHeartbeatFrequency(TimeUnit.MILLISECONDS));

        String stringSettings = MongoConnection.toString(settings);
        assertTrue(stringSettings.contains("maxWaitTime=" + 60000));
        assertTrue(stringSettings.contains("heartbeatFrequency=" + 5000));
    }

    @Test
    public void testDocumentNodeStoreServiceUsesMongoDBDefaults() {
        String testUri = "mongodb://localhost:27017/testdb";
        ConnectionString mongoURI = new ConnectionString(testUri);

        // OAK-11838: Copied from DocumentNodeStoreService.java registerNodeStore() lines 353-356:
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(mongoURI)
                .build();

        ConnectionPoolSettings poolSettings = settings.getConnectionPoolSettings();
        ServerSettings serverSettings = settings.getServerSettings();

        // Uses MongoDB driver defaults (120000), instead of Oak defaults
        assertEquals(120000, (int) poolSettings.getMaxWaitTime(TimeUnit.MILLISECONDS));
        // Uses MongoDB driver defaults (10000), instead of Oak defaults
        assertEquals(10000, (int) serverSettings.getHeartbeatFrequency(TimeUnit.MILLISECONDS));

        String stringSettings = MongoConnection.toString(settings);
        assertTrue(stringSettings.contains("maxWaitTime=" + 120000));
        assertTrue(stringSettings.contains("heartbeatFrequency=" + 10000));
    }

    @Test
    public void testMongoDBDriverConnectionStringParameters() {
        // This test verifies that the MongoDB driver connection string parameters
        // values are randomly set to ensure they are properly applied.
        int heartbeatFrequency = 5144;
        int maxIdleTime = 11221;
        int minPoolSize = 23;
        int maxPoolSize = 78;

        String testUri = "mongodb://localhost:27017/testdb" +
                "?heartbeatFrequencyMS=" + heartbeatFrequency +
                "&maxIdleTimeMS=" + maxIdleTime +
                "&minPoolSize=" + minPoolSize +
                "&maxPoolSize=" + maxPoolSize;
        ConnectionString mongoURI = new ConnectionString(testUri);

        MongoClientSettings mongoSettings = MongoClientSettings.builder()
                .applyConnectionString(mongoURI)
                .build();

        ConnectionPoolSettings mongoPoolSettings = mongoSettings.getConnectionPoolSettings();
        ServerSettings mongoServerSettings = mongoSettings.getServerSettings();

        // Assert all the connection string parameters are correctly applied
        assertEquals(heartbeatFrequency, (int) mongoServerSettings.getHeartbeatFrequency(TimeUnit.MILLISECONDS));
        assertEquals(maxIdleTime, (int) mongoPoolSettings.getMaxConnectionIdleTime(TimeUnit.MILLISECONDS));
        assertEquals(minPoolSize, mongoPoolSettings.getMinSize());
        assertEquals(maxPoolSize, mongoPoolSettings.getMaxSize());

        if (DEBUG) {
            System.out.println("=== testMongoDBDriverConnectionStringParameters ===");
            System.out.println("heartbeatFrequency: " + mongoServerSettings.getHeartbeatFrequency(TimeUnit.MILLISECONDS));
            System.out.println("maxIdleTime: " + mongoPoolSettings.getMaxConnectionIdleTime(TimeUnit.MILLISECONDS));
            System.out.println("minPoolSize: " + mongoPoolSettings.getMinSize());
            System.out.println("maxPoolSize: " + mongoPoolSettings.getMaxSize());
            System.out.println("Settings: " + MongoConnection.toString(mongoSettings));
        }
    }

    @Test
    public void testMongoDBDriverProgrammaticParameters() {
        // This test verifies that MongoDB driver parameters can be set programmatically
        // using builder methods with randomized but sensible values.
        int connectTimeout = 8765;
        int socketTimeout = 15432;
        int maxWaitTime = 45000;
        int maxConnectionLifeTime = 61442;
        int maintenanceInitialDelay = 2234;
        int maintenanceFrequency = 28102;
        int heartbeatFrequency = 7890;
        int maxIdleTime = 18543;
        int minPoolSize = 12;
        int maxPoolSize = 67;

        String testUri = "mongodb://localhost:27017/testdb";
        ConnectionString mongoURI = new ConnectionString(testUri);

        MongoClientSettings mongoSettings = MongoClientSettings.builder()
                .applyConnectionString(mongoURI)
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
                        .readTimeout(socketTimeout, TimeUnit.MILLISECONDS))
                .applyToConnectionPoolSettings(builder -> builder
                        .maxWaitTime(maxWaitTime, TimeUnit.MILLISECONDS)
                        .maxConnectionLifeTime(maxConnectionLifeTime, TimeUnit.MILLISECONDS)
                        .maintenanceInitialDelay(maintenanceInitialDelay, TimeUnit.MILLISECONDS)
                        .maintenanceFrequency(maintenanceFrequency, TimeUnit.MILLISECONDS)
                        .maxConnectionIdleTime(maxIdleTime, TimeUnit.MILLISECONDS)
                        .minSize(minPoolSize)
                        .maxSize(maxPoolSize))
                .applyToServerSettings(builder -> builder
                        .heartbeatFrequency(heartbeatFrequency, TimeUnit.MILLISECONDS))
                .build();

        ConnectionPoolSettings mongoPoolSettings = mongoSettings.getConnectionPoolSettings();
        ServerSettings mongoServerSettings = mongoSettings.getServerSettings();

        // Assert programmatically set parameters
        assertEquals(connectTimeout, mongoSettings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS));
        assertEquals(socketTimeout, mongoSettings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
        assertEquals(maxWaitTime, mongoPoolSettings.getMaxWaitTime(TimeUnit.MILLISECONDS));
        assertEquals(maxConnectionLifeTime, mongoPoolSettings.getMaxConnectionLifeTime(TimeUnit.MILLISECONDS));
        assertEquals(maintenanceInitialDelay, mongoPoolSettings.getMaintenanceInitialDelay(TimeUnit.MILLISECONDS));
        assertEquals(maintenanceFrequency, mongoPoolSettings.getMaintenanceFrequency(TimeUnit.MILLISECONDS));

        // Assert the 4 parameters that were also in the connection string test
        assertEquals(heartbeatFrequency, (int) mongoServerSettings.getHeartbeatFrequency(TimeUnit.MILLISECONDS));
        assertEquals(maxIdleTime, (int) mongoPoolSettings.getMaxConnectionIdleTime(TimeUnit.MILLISECONDS));
        assertEquals(minPoolSize, mongoPoolSettings.getMinSize());
        assertEquals(maxPoolSize, mongoPoolSettings.getMaxSize());

        if (DEBUG) {
            System.out.println("=== testMongoDBDriverProgrammaticParameters ===");
            System.out.println("connectTimeout: " + mongoSettings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS));
            System.out.println("socketTimeout: " + mongoSettings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
            System.out.println("maxWaitTime: " + mongoPoolSettings.getMaxWaitTime(TimeUnit.MILLISECONDS));
            System.out.println("maxConnectionLifeTime: " + mongoPoolSettings.getMaxConnectionLifeTime(TimeUnit.MILLISECONDS));
            System.out.println("maintenanceInitialDelay: " + mongoPoolSettings.getMaintenanceInitialDelay(TimeUnit.MILLISECONDS));
            System.out.println("maintenanceFrequency: " + mongoPoolSettings.getMaintenanceFrequency(TimeUnit.MILLISECONDS));
            System.out.println("heartbeatFrequency: " + mongoServerSettings.getHeartbeatFrequency(TimeUnit.MILLISECONDS));
            System.out.println("maxIdleTime: " + mongoPoolSettings.getMaxConnectionIdleTime(TimeUnit.MILLISECONDS));
            System.out.println("minPoolSize: " + mongoPoolSettings.getMinSize());
            System.out.println("maxPoolSize: " + mongoPoolSettings.getMaxSize());
            System.out.println("Settings: " + MongoConnection.toString(mongoSettings));
        }
    }
}
