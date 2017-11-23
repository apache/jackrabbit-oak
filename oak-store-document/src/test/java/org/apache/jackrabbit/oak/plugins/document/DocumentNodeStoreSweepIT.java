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

import java.util.SortedMap;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class DocumentNodeStoreSweepIT extends AbstractTwoNodeTest {

    private FailingDocumentStore store1;
    private FailingDocumentStore store2;

    public DocumentNodeStoreSweepIT(DocumentStoreFixture fixture) {
        super(fixture);
        // this test checks cache invalidation, which is
        // not available with the MemoryDocumentStore
        assumeFalse(fixture instanceof DocumentStoreFixture.MemoryFixture);
    }

    @Override
    protected DocumentStore customize(DocumentStore store) {
        DocumentStore s;
        if (store1 == null) {
            store1 = new FailingDocumentStore(store, 42);
            s = store1;
        } else if (store2 == null) {
            store2 = new FailingDocumentStore(store, 42);
            s = store2;
        } else {
            throw new RuntimeException("too many stores initialized");
        }
        return s;
    }

    @Test
    public void invalidateAfterSelfRecovery() throws Exception {
        String path = createUncommittedChanges(ds1, store1);

        // cluster node 2 must see uncommitted change
        assertFalse(isClean(ds2, path));

        // crash and restart cluster node 1, this will run recovery
        // and revert uncommitted changes
        ds1 = crashAndRestart(ds1, store1);

        // must see uncommitted change, because no background read occurred
        assertFalse(isClean(ds2, path));

        ds2.runBackgroundReadOperations();

        // now cache must reflect the up-to-date document
        assertTrue(isClean(ds2, path));
    }

    @Test
    public void invalidateAfterRecovery() throws Exception {
        String path = createUncommittedChanges(ds1, store1);

        // cluster node 2 must see uncommitted change
        assertFalse(isClean(ds2, path));

        // crash cluster node 1
        crash(ds1, store1);

        // must still see uncommitted change
        assertFalse(isClean(ds2, path));

        // wait for lease to expire
        clock.waitUntil(clock.getTime() + ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS);
        // run recovery for cluster node 1
        assertTrue(ds2.getLastRevRecoveryAgent().recover(c1Id) > 0);

        // now cache must reflect the up-to-date document
        assertTrue(isClean(ds2, path));
    }


    private static boolean isClean(DocumentNodeStore ns, String path) {
        // use find that also reads from the cache
        NodeDocument doc = ns.getDocumentStore().find(NODES, Utils.getIdFromPath(path));
        for (Revision c : doc.getAllChanges()) {
            String commitValue = ns.getCommitValue(c, doc);
            if (!Utils.isCommitted(commitValue)) {
                return false;
            }
        }
        return true;
    }

    private static DocumentNodeStore crashAndRestart(DocumentNodeStore ns,
                                                     FailingDocumentStore store) {
        DocumentStore s = ns.getDocumentStore();
        BlobStore bs = ns.getBlobStore();
        int clusterId = ns.getClusterId();
        int asyncDelay = ns.getAsyncDelay();
        Clock clock = ns.getClock();

        crash(ns, store);

        return new DocumentMK.Builder().setBlobStore(bs).setDocumentStore(s)
                .setClusterId(clusterId).clock(clock).setAsyncDelay(asyncDelay)
                .setLeaseCheck(false).getNodeStore();
    }

    private static void crash(DocumentNodeStore ns,
                              FailingDocumentStore store) {
        store.fail().after(0).eternally();
        try {
            ns.dispose();
            fail("dispose() must fail with an exception");
        } catch (DocumentStoreException e) {
            // expected
        }
        store.fail().never();
    }

    private String createUncommittedChanges(DocumentNodeStore ns,
                                          FailingDocumentStore store) throws Exception {
        ns.setMaxBackOffMillis(0);
        NodeBuilder builder = ns.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            builder.child("node-" + i);
        }

        store.fail().after(5).eternally();
        try {
            merge(ns, builder);
            fail("must fail with exception");
        } catch (CommitFailedException e) {
            // expected
        }
        store.fail().never();

        // store must now contain uncommitted changes
        NodeDocument doc = null;
        for (NodeDocument d : Utils.getAllDocuments(store)) {
            if (d.getPath().startsWith("/node-")) {
                doc = d;
                break;
            }
        }
        assertNotNull(doc);
        assertNull(doc.getNodeAtRevision(ns, ns.getHeadRevision(), null));
        SortedMap<Revision, String> deleted = doc.getLocalDeleted();
        assertEquals(1, deleted.size());
        assertNull(ns.getCommitValue(deleted.firstKey(), doc));

        return doc.getPath();
    }

}
