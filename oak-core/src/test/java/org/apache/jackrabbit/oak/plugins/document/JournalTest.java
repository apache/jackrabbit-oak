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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.junit.LogDumper;
import org.apache.jackrabbit.oak.commons.junit.LogLevelModifier;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JournalTest extends AbstractJournalTest {

    private MemoryDocumentStore ds;
    private MemoryBlobStore bs;

    @Rule
    public TestRule logDumper = new LogDumper();

    @Rule
    public TestRule logLevelModifier = new LogLevelModifier()
                                            .addAppenderFilter("file", "warn")
                                            .setLoggerLevel("org.apache.jackrabbit.oak", "trace");

    class DiffingObserver implements Observer, Runnable, NodeStateDiff {

        final List<DocumentNodeState> incomingRootStates1 = Lists.newArrayList();
        final List<DocumentNodeState> diffedRootStates1 = Lists.newArrayList();
        
        DocumentNodeState oldRoot = null;
        
        DiffingObserver(boolean startInBackground) {
            if (startInBackground) {
                // start the diffing in the background - so as to not
                // interfere with the contentChanged call
                Thread th = new Thread(this);
                th.setDaemon(true);
                th.start();
            }
        }

        public void clear() {
            synchronized(incomingRootStates1) {
                incomingRootStates1.clear();
                diffedRootStates1.clear();
            }
        }
        
        @Override
        public void contentChanged(@Nonnull NodeState root,@Nonnull CommitInfo info) {
            synchronized(incomingRootStates1) {
                incomingRootStates1.add((DocumentNodeState) root);
                incomingRootStates1.notifyAll();
            }
        }
        
        public void processAll() {
            while(processOne()) {
                // continue
            }
        }

        public boolean processOne() {
            DocumentNodeState newRoot;
            synchronized(incomingRootStates1) {
                if (incomingRootStates1.size()==0) {
                    return false;
                }
                newRoot = incomingRootStates1.remove(0);
            }
            if (oldRoot!=null) {
                newRoot.compareAgainstBaseState(oldRoot, this);
            }
            oldRoot = newRoot;
            synchronized(incomingRootStates1) {
                diffedRootStates1.add(newRoot);
            }
            return true;
        }
        
        @Override
        public void run() {
            while(true) {
                DocumentNodeState newRoot;
                synchronized(incomingRootStates1) {
                    while(incomingRootStates1.size()==0) {
                        try {
                            incomingRootStates1.wait();
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                    newRoot = incomingRootStates1.remove(0);
                }
                if (oldRoot!=null) {
                    newRoot.compareAgainstBaseState(oldRoot, this);
                }
                oldRoot = newRoot;
                synchronized(incomingRootStates1) {
                    diffedRootStates1.add(newRoot);
                }
            }
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            return true;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            return true;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before,
                NodeState after) {
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            return true;
        }

        public int getTotal() {
            synchronized(incomingRootStates1) {
                return incomingRootStates1.size() + diffedRootStates1.size();
            }
        }
        
    }
    
    @Test
    public void cleanupTest() throws Exception {
        DocumentMK mk1 = createMK(0 /* clusterId: 0 => uses clusterNodes collection */, 0);
        DocumentNodeStore ns1 = mk1.getNodeStore();
        // make sure we're visible and marked as active
        ns1.renewClusterIdLease();
        // first clean up
        Thread.sleep(100); // OAK-2979 : wait 100ms before doing the cleanup
        jGC(ns1, 1, TimeUnit.MILLISECONDS);
        Thread.sleep(100); // sleep just quickly
        assertEquals(0, jGC(ns1, 1, TimeUnit.DAYS));
        assertEquals(0, jGC(ns1, 6, TimeUnit.HOURS));
        assertEquals(0, jGC(ns1, 1, TimeUnit.HOURS));
        assertEquals(0, jGC(ns1, 10, TimeUnit.MINUTES));
        assertEquals(0, jGC(ns1, 1, TimeUnit.MINUTES));
        assertEquals(0, jGC(ns1, 1, TimeUnit.SECONDS));
        assertEquals(0, jGC(ns1, 1, TimeUnit.MILLISECONDS));
        
        // create some entries that can be deleted thereupon
        mk1.commit("/", "+\"regular1\": {}", null, null);
        mk1.commit("/", "+\"regular2\": {}", null, null);
        mk1.commit("/", "+\"regular3\": {}", null, null);
        mk1.commit("/regular2", "+\"regular4\": {}", null, null);
        Thread.sleep(100); // sleep 100millis
        assertEquals(0, jGC(ns1, 5, TimeUnit.SECONDS));
        assertEquals(0, jGC(ns1, 1, TimeUnit.MILLISECONDS));
        ns1.runBackgroundOperations();
        mk1.commit("/", "+\"regular5\": {}", null, null);
        ns1.runBackgroundOperations();
        mk1.commit("/", "+\"regular6\": {}", null, null);
        ns1.runBackgroundOperations();
        Thread.sleep(100); // sleep 100millis
        assertEquals(0, jGC(ns1, 5, TimeUnit.SECONDS));
        assertEquals(3, jGC(ns1, 1, TimeUnit.MILLISECONDS));
    }
    
    @Test
    public void journalTest() throws Exception {
        DocumentMK mk1 = createMK(1, 0);
        DocumentNodeStore ns1 = mk1.getNodeStore();
        CountingDocumentStore countingDocStore1 = builder.actualStore;
        CountingTieredDiffCache countingDiffCache1 = builder.actualDiffCache;

        DocumentMK mk2 = createMK(2, 0);
        DocumentNodeStore ns2 = mk2.getNodeStore();
        CountingDocumentStore countingDocStore2 = builder.actualStore;
        CountingTieredDiffCache countingDiffCache2 = builder.actualDiffCache;

        final DiffingObserver observer = new DiffingObserver(false);
        ns1.addObserver(observer);
        
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();
        observer.processAll(); // to make sure we have an 'oldRoot'
        observer.clear();
        countingDocStore1.resetCounters();
        countingDocStore2.resetCounters();
        // countingDocStore1.printStacks = true;
        countingDiffCache1.resetLoadCounter();
        countingDiffCache2.resetLoadCounter();

        mk2.commit("/", "+\"regular1\": {}", null, null);
        mk2.commit("/", "+\"regular2\": {}", null, null);
        mk2.commit("/", "+\"regular3\": {}", null, null);
        mk2.commit("/regular2", "+\"regular4\": {}", null, null);
        // flush to journal
        ns2.runBackgroundOperations();
        
        // nothing notified yet
        assertEquals(0, observer.getTotal());
        assertEquals(0, countingDocStore1.getNumFindCalls(Collection.NODES));
        assertEquals(0, countingDocStore1.getNumQueryCalls(Collection.NODES));
        assertEquals(0, countingDocStore1.getNumRemoveCalls(Collection.NODES));
        assertEquals(0, countingDocStore1.getNumCreateOrUpdateCalls(Collection.NODES));
        assertEquals(0, countingDiffCache1.getLoadCount());
        
        // let node 1 read those changes
        // System.err.println("run background ops");
        ns1.runBackgroundOperations();
        mk2.commit("/", "+\"regular5\": {}", null, null);
        ns2.runBackgroundOperations();
        ns1.runBackgroundOperations();
        // and let the observer process everything
        observer.processAll();
        countingDocStore1.printStacks = false;
        
        // now expect 1 entry in rootStates
        assertEquals(2, observer.getTotal());
        assertEquals(0, countingDiffCache1.getLoadCount());
        assertEquals(0, countingDocStore1.getNumRemoveCalls(Collection.NODES));
        assertEquals(0, countingDocStore1.getNumCreateOrUpdateCalls(Collection.NODES));
        assertEquals(0, countingDocStore1.getNumQueryCalls(Collection.NODES));
//        assertEquals(0, countingDocStore1.getNumFindCalls(Collection.NODES));
    }
    
    @Test
    public void externalBranchChange() throws Exception {
        DocumentMK mk1 = createMK(1, 0);
        DocumentNodeStore ns1 = mk1.getNodeStore();
        DocumentMK mk2 = createMK(2, 0);
        DocumentNodeStore ns2 = mk2.getNodeStore();
        
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        mk1.commit("/", "+\"regular1\": {}", null, null);
        // flush to journal
        ns1.runBackgroundOperations();
        mk1.commit("/regular1", "+\"regular1child\": {}", null, null);
        // flush to journal
        ns1.runBackgroundOperations();
        mk1.commit("/", "+\"regular2\": {}", null, null);
        // flush to journal
        ns1.runBackgroundOperations();
        mk1.commit("/", "+\"regular3\": {}", null, null);
        // flush to journal
        ns1.runBackgroundOperations();
        mk1.commit("/", "+\"regular4\": {}", null, null);
        // flush to journal
        ns1.runBackgroundOperations();
        mk1.commit("/", "+\"regular5\": {}", null, null);
        // flush to journal
        ns1.runBackgroundOperations();
        String b1 = mk1.branch(null);
        b1 = mk1.commit("/", "+\"branchVisible\": {}", b1, null);
        mk1.merge(b1, null);
        
        // to flush the branch commit either dispose of mk1
        // or run the background operations explicitly 
        // (as that will propagate the lastRev to the root)
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();
        
        String nodes = mk2.getNodes("/", null, 0, 0, 100, null);
        assertEquals("{\"branchVisible\":{},\"regular1\":{},\"regular2\":{},\"regular3\":{},\"regular4\":{},\"regular5\":{},\":childNodeCount\":6}", nodes);
    }
    
    /** Inspired by LastRevRecoveryTest.testRecover() - simplified and extended with journal related asserts **/
    @Test
    public void lastRevRecoveryJournalTest() throws Exception {
        doLastRevRecoveryJournalTest(false);
    }
    
    /** Inspired by LastRevRecoveryTest.testRecover() - simplified and extended with journal related asserts **/
    @Test
    public void lastRevRecoveryJournalTestWithConcurrency() throws Exception {
        doLastRevRecoveryJournalTest(true);
    }
    
    private void doLastRevRecoveryJournalTest(boolean testConcurrency) throws Exception {
        DocumentMK mk1 = createMK(1, 0);
        DocumentNodeStore ds1 = mk1.getNodeStore();
        int c1Id = ds1.getClusterId();
        DocumentMK mk2 = createMK(2, 0);
        DocumentNodeStore ds2 = mk2.getNodeStore();
        final int c2Id = ds2.getClusterId();
        
        // should have none yet
        assertEquals(0, countJournalEntries(ds1, 10));
        assertEquals(0, countJournalEntries(ds2, 10));
        
        //1. Create base structure /x/y
        NodeBuilder b1 = ds1.getRoot().builder();
        b1.child("x").child("y");
        ds1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ds1.runBackgroundOperations();

        //lastRev are persisted directly for new nodes. In case of
        // updates they are persisted via background jobs

        //1.2 Get last rev populated for root node for ds2
        ds2.runBackgroundOperations();
        NodeBuilder b2 = ds2.getRoot().builder();
        b2.child("x").setProperty("f1","b1");
        ds2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ds2.runBackgroundOperations();

        //2. Add a new node /x/y/z
        b2 = ds2.getRoot().builder();
        b2.child("x").child("y").child("z").setProperty("foo", "bar");
        ds2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Refresh DS1
        ds1.runBackgroundOperations();

        final NodeDocument z1 = getDocument(ds1, "/x/y/z");
        NodeDocument y1 = getDocument(ds1, "/x/y");
        final NodeDocument x1 = getDocument(ds1, "/x");

        Revision zlastRev2 = z1.getLastRev().get(c2Id);
        // /x/y/z is a new node and does not have a _lastRev
        assertNull(zlastRev2);
        Revision head2 = ds2.getHeadRevision().getRevision(ds2.getClusterId());

        //lastRev should not be updated for C #2
        assertNull(y1.getLastRev().get(c2Id));

        final LastRevRecoveryAgent recovery = new LastRevRecoveryAgent(ds1);

        // now 1 also has
        final String change1 = "{\"x\":{\"y\":{}}}";
        assertJournalEntries(ds1, change1);
        final String change2 = "{\"x\":{}}";
        assertJournalEntries(ds2, change2);


        String change2b = "{\"x\":{\"y\":{\"z\":{}}}}";

        if (!testConcurrency) {
            //Do not pass y1 but still y1 should be updated
            recovery.recover(Lists.newArrayList(x1,z1), c2Id);
    
            //Post recovery the lastRev should be updated for /x/y and /x
            assertEquals(head2, getDocument(ds1, "/x/y").getLastRev().get(c2Id));
            assertEquals(head2, getDocument(ds1, "/x").getLastRev().get(c2Id));
            assertEquals(head2, getDocument(ds1, "/").getLastRev().get(c2Id));
    
            // now 1 is unchanged, but 2 was recovered now, so has one more:
            assertJournalEntries(ds1, change1); // unchanged
            assertJournalEntries(ds2, change2, change2b);
            
            // just some no-ops:
            recovery.recover(c2Id);
            recovery.recover(Collections.<NodeDocument>emptyList(), c2Id);
            assertJournalEntries(ds1, change1); // unchanged
            assertJournalEntries(ds2, change2, change2b);

        } else {
        
            // do some concurrency testing as well to check if 
            final int NUM_THREADS = 200;
            final CountDownLatch ready = new CountDownLatch(NUM_THREADS);
            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch end = new CountDownLatch(NUM_THREADS);
            final List<Exception> exceptions = synchronizedList(new ArrayList<Exception>());
            for (int i = 0; i < NUM_THREADS; i++) {
                Thread th = new Thread(new Runnable() {
    
                    @Override
                    public void run() {
                        try {
                            ready.countDown();
                            start.await();
                            recovery.recover(Lists.newArrayList(x1,z1), c2Id);
                        } catch (Exception e) {
                            exceptions.add(e);
                        } finally {
                            end.countDown();
                        }
                    }
                    
                });
                th.start();
            }
            ready.await(5, TimeUnit.SECONDS);
            start.countDown();
            assertTrue(end.await(20, TimeUnit.SECONDS));
            assertJournalEntries(ds1, change1); // unchanged
            assertJournalEntries(ds2, change2, change2b);
            for (Exception ex : exceptions) {
                throw ex;
            }
        }
    }

    // OAK-3433
    @Test
    public void journalEntryKey() throws Exception {
        DocumentNodeStore ns1 = createMK(1, 0).getNodeStore();
        DocumentNodeStore ns2 = createMK(2, 0).getNodeStore();

        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("foo");
        ns1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns1.runBackgroundOperations();

        NodeBuilder b2 = ns2.getRoot().builder();
        b2.child("bar");
        ns2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        Revision h2 = ns2.getHeadRevision().getRevision(ns2.getClusterId());
        assertNotNull(h2);

        ns2.runBackgroundReadOperations();

        ns2.runBackgroundOperations();

        String id = JournalEntry.asId(h2);
        assertTrue("Background update did not create a journal entry with id " + id,
                ns1.getDocumentStore().find(Collection.JOURNAL, id) != null);
    }

    private int jGC(DocumentNodeStore ns, long maxRevisionAge, TimeUnit unit) {
        return new JournalGarbageCollector(ns, unit.toMillis(maxRevisionAge)).gc();
    }

    private DocumentMK createMK(int clusterId, int asyncDelay) {
        if (ds == null) {
            ds = new MemoryDocumentStore();
        }
        if (bs == null) {
            bs = new MemoryBlobStore();
        }
        return createMK(clusterId, asyncDelay, ds, bs);
    }
}
