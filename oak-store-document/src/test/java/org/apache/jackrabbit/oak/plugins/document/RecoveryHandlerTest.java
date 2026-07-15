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

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RecoveryHandlerTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Clock clock = new Clock.Virtual();
    private FailingDocumentStore store = new FailingDocumentStore(new MemoryDocumentStore());
    private MissingLastRevSeeker seeker = new MissingLastRevSeeker(store, clock);
    private RecoveryHandler handler = new RecoveryHandlerImpl(store, clock, seeker);

    @Before
    public void before() throws Exception {
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
    }

    @AfterClass
    public static void after() {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
    }

    @Test
    public void failWithException() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder().clock(clock)
                .setDocumentStore(store).build();
        int clusterId = ns.getClusterId();

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);

        // crash the node store
        store.fail().after(0).eternally();
        try {
            ns.dispose();
            fail("dispose must fail with exception");
        } catch (DocumentStoreException e) {
            // expected
        }

        // let lease time out
        clock.waitUntil(clock.getTime() + ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS + 1);

        // must not be able to recover with a failing document store
        assertFalse(handler.recover(clusterId));

        store.fail().never();
        // must succeed after store is accessible again
        assertTrue(handler.recover(clusterId));
    }
}
