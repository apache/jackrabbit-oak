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
package org.apache.jackrabbit.oak.plugins.document;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.LeaseCheckDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;

import junitx.util.PrivateAccessor;

public class Sweep2TestHelper {

    static void testPre18UpgradeSimulations(DocumentNodeStore ns, DocumentMKBuilderProvider builderProvider) {
        // make sure the DocumentNodeStore is disposed before testing if this hasn't been done yet
        ns.dispose();

        MemoryDocumentStore memStore = getMemoryDocumentStore(ns);

        Set<Integer> activeIds = new HashSet<Integer>();
        activeIds.add(ns.getClusterId());
        ClusterViewDocument.readOrUpdate(ns, activeIds, null, null);
        doTestPre18Upgrade(memStore.copy(), builderProvider.newBuilder(), false);
        doTestPre18Upgrade(memStore.copy(), builderProvider.newBuilder(), true);
    }

    static MemoryDocumentStore getMemoryDocumentStore(DocumentNodeStore ns) {
        DocumentStore ds = ns.getDocumentStore();
        if (ds instanceof LeaseCheckDocumentStoreWrapper) {
            LeaseCheckDocumentStoreWrapper w = (LeaseCheckDocumentStoreWrapper)ds;
            try {
                ds = (DocumentStore) PrivateAccessor.getField(w, "delegate");
            } catch (NoSuchFieldException e) {
                fail("Could unwrap DocumentStore : " + w);
            }
        }
        if (!(ds instanceof MemoryDocumentStore)) {
            throw new IllegalStateException("only supporting MemoryDocumentStore for this test for now - DocumentStore cloning not yet generally supported");
        }

        MemoryDocumentStore memStore = (MemoryDocumentStore) ds;
        return memStore;
    }

    /**
     * Does a simulation of the following steps:
     * <ol>
     * <li>create content in an oak version pre 1.8</li>
     * <li>if simulatePreFixUpgrade then simulate an upgrade to a version post 1.8 but without a fix for the branch commit problem</li>
     * <li>upgrade to a fixed version</li>
     * </ol>
     * @param ns
     * @param newBuilder
     * @param simulatePreFixUpgrade if true, simulate step 2 as well, if false, leave out that step
     */
    private static void doTestPre18Upgrade(MemoryDocumentStore memStore, DocumentMK.Builder newBuilder, boolean simulatePreFixUpgrade) {
        // step 2 : keep a copy in memStoreOriginal for debugging purpose
        MemoryDocumentStore memStoreOriginal = memStore.copy();

        // step 3: simulate DocumentNodeStore<init> calling branches.init(store, this) calling doc.purgeUncommittedRevisions(context)
        // by doing another restart/shutdown cycle
        MemoryDocumentStore memStoreOriginalAdjusted = memStoreOriginal.copy();
        DocumentNodeStore ns = newBuilder
                .setClusterId(1)
                .setDocumentStore(memStoreOriginalAdjusted).build();
        ns.dispose();

        // step 4 : simulate any existing content changes done so far having happened pre 1.8
        //          by deleting "_bc" and "_sweepRev"
        //          ("_sweepRev" also didn't exist pre 1.8 and otherwise prevents
        //          backgroundSweep()/forceBackgroundSweep()
        revertToPre18State(memStore);

        // step 5 : keep a copy of the store pre18 upgrade for debugging purpose
        @SuppressWarnings("unused")
        MemoryDocumentStore memStorePre18Upgrade = memStore.copy();

        // optional step 6 : simulate an upgrade to post 1.8 without a fix yet
        if (simulatePreFixUpgrade) {
            NodeDocumentSweeper.SWEEP_ONE_PREDICATE = Utils.PROPERTY_OR_DELETED;
            ns = newBuilder
                    .setClusterId(1)
                    .setDocumentStore(memStore).build();
            ns.dispose();
            // put back the fix asap
            NodeDocumentSweeper.SWEEP_ONE_PREDICATE = Utils.PROPERTY_OR_DELETED_OR_COMMITROOT_OR_REVISIONS;

            // remove the sweep2Status - this is what manages whether the sweep2 was applied or not
            removeSweep2Status(memStore);
        }

        // step 7 : startup new instance (1.8+) - thanks to the above simulatePre18Behaviour
        //          this will now run backgroundSweep()/forceBackgroundSweep()
        ns = newBuilder
                .setAsyncDelay(1)
                .setClusterId(1)
                .setDocumentStore(memStore).build();

        assertTrue("sweep2 was too slow", waitForSweep2Done(ns, 10000));

        for (NodeDocument originalNode : memStoreOriginalAdjusted.query(NODES, NodeDocument.MIN_ID_VALUE, NodeDocument.MAX_ID_VALUE, Integer.MAX_VALUE)) {
            NodeDocument actualNode = memStore.find(NODES, originalNode.getId());

            assertBranchCommitsEqual(ns, ns.getSweepRevisions(), originalNode, actualNode);
        }
    }

    private static boolean waitForSweep2Done(DocumentNodeStore ns, long maxWaitMillis) {
        final long timeout = System.currentTimeMillis() + maxWaitMillis;
        while(System.currentTimeMillis() < timeout) {
            Sweep2StatusDocument sweep2StatusDoc = Sweep2StatusDocument.readFrom(ns.getDocumentStore());
            if (sweep2StatusDoc != null && sweep2StatusDoc.isSwept()) {
                return true;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                fail("interruptedException: " + e);
            }
        }
        return false;
    }

    private static void assertBranchCommitsEqual(RevisionContext rc, RevisionVector sweepRevs, NodeDocument expectedNode, NodeDocument actualNode) {
        Set<Revision> expectedBcs = new HashSet<>(expectedNode.getValueMap("_bc").keySet());
        Set<Revision> actualBcs = new HashSet<>(actualNode.getValueMap("_bc").keySet());
        if (!expectedBcs.equals(actualBcs) && expectedBcs.containsAll(actualBcs)) {
            // there might have to be more bc than we actually have.
            // check if they have been swept
            Iterator<Revision> it = expectedBcs.iterator();
            while(it.hasNext()) {
                Revision expectedBc = it.next();
                if (!sweepRevs.isRevisionNewer(expectedBc)) {
                    it.remove();
                }
            }
            it = actualBcs.iterator();
            while(it.hasNext()) {
                Revision actualBc = it.next();
                if (!sweepRevs.isRevisionNewer(actualBc)) {
                    it.remove();
                }
            }
            if (expectedBcs.equals(actualBcs)) {
                // perfect
                return;
            }
        }
        assertEquals("\"_bc\" mismatch on " + expectedNode.getId(), expectedNode.getValueMap("_bc").keySet(), actualNode.getValueMap("_bc").keySet());
    }

    /**
     * Remove data from the store bringing it to a pre Oak 1.8 state
     * @param store
     */
    static void revertToPre18State(DocumentStore store) {
        // A : remove all "_bc"
        for (NodeDocument nodeDocument :
            store.query(NODES, NodeDocument.MIN_ID_VALUE, NodeDocument.MAX_ID_VALUE, Integer.MAX_VALUE)) {
            UpdateOp removeBc = new UpdateOp(nodeDocument.getId(), false);
            removeBc.remove("_bc");
            assertNotNull(store.findAndUpdate(Collection.NODES, removeBc));
        }
        // B : remove "_sweepRev"
        UpdateOp removeSweepRev = new UpdateOp(Utils.getIdFromPath("/"), false);
        removeSweepRev.remove("_sweepRev");
        assertNotNull(store.findAndUpdate(Collection.NODES, removeSweepRev));
        // C : remove the "sweep2Status" from the settings
        removeSweep2Status(store);
    }

    static void removeSweep2Status(DocumentStore store) {
        store.remove(Collection.SETTINGS, "sweep2Status");
    }

    static DocumentNodeStore applyPre18Aging(DocumentStore memStore, DocumentMKBuilderProvider builderProvider, int newClusterId) {
        return applyPre18Aging(memStore, builderProvider, newClusterId, -1);
    }

    static DocumentNodeStore applyPre18Aging(DocumentStore memStore, DocumentMKBuilderProvider builderProvider, int newClusterId, int asyncDelay) {
        revertToPre18State(memStore);

        // mark as swept2 to avoid a proper sweep2, only makes sense for testing
        Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(memStore, 1);

        NodeDocumentSweeper.SWEEP_ONE_PREDICATE = Utils.PROPERTY_OR_DELETED;
        Builder builder = builderProvider.newBuilder();
        if (asyncDelay != -1) {
            builder.setAsyncDelay(asyncDelay);
        }
        DocumentNodeStore ns = builder
                .setClusterId(newClusterId)
                .setDocumentStore(memStore).build();
        NodeDocumentSweeper.SWEEP_ONE_PREDICATE = Utils.PROPERTY_OR_DELETED_OR_COMMITROOT_OR_REVISIONS;

        return ns;
    }
}
