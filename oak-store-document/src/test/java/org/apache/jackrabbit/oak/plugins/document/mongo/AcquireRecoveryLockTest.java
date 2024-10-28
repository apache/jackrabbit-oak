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

import java.util.List;

import com.mongodb.MongoClient;

import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo;
import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfoDocument;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.LeaseCheckMode;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

public class AcquireRecoveryLockTest extends AbstractMongoConnectionTest {

    private Clock clock = new Clock.Virtual();

    private MongoDocumentStore store;

    @Before
    public void before() throws Exception {
        clock.waitUntil(System.currentTimeMillis());
        MongoConnection connection = connectionFactory.getConnection();
        assumeNotNull(connection);
        store = new MongoDocumentStore(
                connection.getMongoClient(), connection.getDatabase(),
                new DocumentMK.Builder());
    }

    @After
    public void after() {
        store.dispose();
    }

    @Override
    protected DocumentMK.Builder newBuilder(MongoClient client, String dbName) throws Exception {
        // disable lease check because test waits until lease times out
        return super.newBuilder(client, dbName).setLeaseCheckMode(LeaseCheckMode.DISABLED);
    }

    @Override
    protected Clock getTestClock() throws InterruptedException {
        return clock;
    }

    // OAK-4131
    @Test
    public void recoveryBy() throws Exception {
        MongoMissingLastRevSeeker seeker = new MongoMissingLastRevSeeker(store, getTestClock());
        List<ClusterNodeInfoDocument> infoDocs = CollectionUtils.toList(seeker.getAllClusters());
        assertEquals(1, infoDocs.size());
        int clusterId = infoDocs.get(0).getClusterId();
        int otherClusterId = clusterId + 1;
        getTestClock().waitUntil(getTestClock().getTime() + ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS + 1000);
        assertTrue(seeker.acquireRecoveryLock(clusterId, otherClusterId));
        ClusterNodeInfoDocument doc = seeker.getClusterNodeInfo(clusterId);
        Object recoveryBy = doc.get(ClusterNodeInfo.REV_RECOVERY_BY);
        assertNotNull(recoveryBy);
        assertEquals(Long.class, recoveryBy.getClass());
    }
}
