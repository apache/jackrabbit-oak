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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BackgroundSplitFailureTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Clock clock = new Clock.Virtual();

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
    public void journalException() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        FailingDocumentStore failingStore = new FailingDocumentStore(store);
        DocumentNodeStore ns = new DocumentNodeStoreBuilder<>()
                .setDocumentStore(failingStore).setAsyncDelay(0).clock(clock).build();
        int clusterId = ns.getClusterId();
        Path fooPath = new Path(Path.ROOT, "foo");
        String fooId = Utils.getIdFromPath(fooPath);

        NodeBuilder builder = ns.getRoot().builder();
        builder.child(fooPath.getName()).setProperty("p", -1);
        merge(ns, builder);
        for (int i = 0; i <= NodeDocument.NUM_REVS_THRESHOLD; i++) {
            builder = ns.getRoot().builder();
            builder.child(fooPath.getName()).setProperty("p", i);
            merge(ns, builder);
        }

        // shut down without running background ops
        failingStore.fail().after(0).eternally();
        try {
            ns.dispose();
            fail("dispose is expected to fail");
        } catch (Exception e) {
            // expected
        }

        // must not have previous documents yet
        NodeDocument foo = store.find(Collection.NODES, fooId);
        assertNotNull(foo);
        assertEquals(0, foo.getPreviousRanges().size());

        // start again with test store
        final AtomicBoolean falseOnJournalEntryCreate = new AtomicBoolean();
        DocumentStore testStore = new DocumentStoreWrapper(store) {
            @Override
            public <T extends Document> boolean create(Collection<T> collection,
                                                       List<UpdateOp> updateOps) {
                if (collection == Collection.JOURNAL
                        && falseOnJournalEntryCreate.get()) {
                    return false;
                }
                return super.create(collection, updateOps);
            }
        };
        ns = builderProvider.newBuilder().setClusterId(clusterId)
                .setDocumentStore(testStore).setAsyncDelay(0).clock(clock).build();
        ns.addSplitCandidate(Utils.getIdFromPath(new Path(Path.ROOT, "foo")));
        falseOnJournalEntryCreate.set(true);
        try {
            ns.runBackgroundOperations();
            fail("background operations are expected to fail");
        } catch (DocumentStoreException e) {
            assertThat(e.getMessage(), containsString("Unable to create journal entry"));
        }

        // must still not have previous documents
        foo = store.find(Collection.NODES, fooId);
        assertNotNull(foo);
        assertEquals(0, foo.getPreviousRanges().size());
        assertTrue(ns.getSplitCandidates().contains(fooId));

        falseOnJournalEntryCreate.set(false);
        ns.runBackgroundOperations();

        // now there must be a split document
        foo = store.find(Collection.NODES, fooId);
        assertNotNull(foo);
        assertEquals(1, foo.getPreviousRanges().size());
        assertFalse(ns.getSplitCandidates().contains(fooId));
    }
}
