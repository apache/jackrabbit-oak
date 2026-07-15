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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RecoveryWithIncorrectClockTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private final DocumentStore store = new MemoryDocumentStore();
    private final FailingDocumentStore s1 = new FailingDocumentStore(store);

    private final Clock clock = new Clock.Virtual();

    private DocumentNodeStore ns1;

    private DocumentNodeStore ns2;

    @Before
    public void before() throws Exception {
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
        ns1 = createDocumentNodeStore(1, s1);
        ns2 = createDocumentNodeStore(2, store);
    }

    @AfterClass
    public static void after() {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
    }

    @Test
    public void recover() throws Exception {
        // prevent ns1 from writing anything. This will also prevent any
        // further updates to clusterNodes entry 1
        s1.fail().after(0).eternally();

        long now = clock.getTime();
        // simulate a sudden jump of time to the system clock
        clock.waitUntil(now + TimeUnit.HOURS.toMillis(1));
        long ahead = clock.getTime();
        // ns2 now sees an expired lease for clusterId 1
        LastRevRecoveryAgent agent = ns2.getLastRevRecoveryAgent();
        try {
            assertTrue(agent.isRecoveryNeeded());
            agent.performRecoveryIfNeeded();
            fail("recovery must fail with expired lease");
        } catch (DocumentStoreException e) {
            // this is expected. any of the above LastRevRecoveryAgent calls
            // can and should fail because the lease for ns2 expired
        }

        ClusterNodeInfoDocument infoDoc = store.find(CLUSTER_NODES, "1");
        assertNotNull(infoDoc);
        // leaseEnd for clusterId must not have advanced to
        // time of clock that jumped ahead
        assertThat(infoDoc.getLeaseEndTime(), lessThan(ahead));
    }

    private DocumentNodeStore createDocumentNodeStore(int clusterId,
                                                      DocumentStore s) {
        return builderProvider.newBuilder()
                .setDocumentStore(s)
                .clock(clock)
                .setClusterId(clusterId)
                .build();
    }
}
