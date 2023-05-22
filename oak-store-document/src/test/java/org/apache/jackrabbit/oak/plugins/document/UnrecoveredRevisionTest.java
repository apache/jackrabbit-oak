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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.LeaseCheckDocumentStoreWrapper;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import junitx.util.PrivateAccessor;

/**
 * Tests unrecovered revisions on a document
 */
public class UnrecoveredRevisionTest {
    
    interface BreakpointCallback {
        
        void breakpointCallback(String contextMsg);
        
    }
    
    class ControllableMemoryDocumentStore extends MemoryDocumentStore {
        
        private final AtomicInteger remainingOps;
        private BreakpointCallback callback;
        private Set<Collection<?>> restrictedCollections = new HashSet<>();
        private List<UpdateOp> writtenUpdateOps = new LinkedList<>();
        
        ControllableMemoryDocumentStore(int remainingOps, BreakpointCallback callback) {
            this.remainingOps = new AtomicInteger(remainingOps);
            this.callback = callback;
            // restrict only NODES by default
            this.restrictedCollections.add(Collection.NODES);
        }
        
        public void clearOps() {
            writtenUpdateOps.clear();
        }

        public void printOps() {
            for (UpdateOp updateOp : writtenUpdateOps) {
                System.out.println("written updateOp: " + updateOp.toString());
            }
        }
        
        private void registerWrittenUpdateOp(Collection<?> collection, UpdateOp updateOp) {
            if (restrictedCollections.contains(collection)) {
                writtenUpdateOps.add(updateOp);
            }
        }

        private void registerWrittenUpdateOp(Collection<?> collection, List<UpdateOp> trimmedOps) {
            if (restrictedCollections.contains(collection)) {
                writtenUpdateOps.addAll(trimmedOps);
            }
        }

        private void setBreakpointCallback(BreakpointCallback callback) {
            this.callback = callback;
        }
        
        @SuppressWarnings("unused")
        private void setRestrictedCollections(Collection<?>... collections) {
            restrictedCollections.clear();
            restrictedCollections.addAll(Arrays.asList(collections));
        }
        
        private void setRestrictedCollections(Set<Collection<?>> collections) {
            restrictedCollections.clear();
            restrictedCollections.addAll(collections);
        }
        
        public Set<Collection<?>> getRestrictedCollections() {
            return new HashSet<>(restrictedCollections);
        }

        public AtomicInteger getRemainingOps() {
            return remainingOps;
        }
        
        private void breakpoint(String msg) {
            callback.breakpointCallback(msg);
        }

        private void allowOp(Collection<?> collection) {
            if (!restrictedCollections.isEmpty() && !restrictedCollections.contains(collection)) {
                return;
            }
            int remaining = remainingOps.decrementAndGet();
            if (remaining < 0) {
                breakpoint("remaining is " + remaining);
            }
        }
        
        private int allowedOps(Collection<?> collection, int desiredOps) {
            if (!restrictedCollections.isEmpty() && !restrictedCollections.contains(collection)) {
                return desiredOps;
            }
            int current = remainingOps.get();
            int remaining = current - desiredOps;
            if (remaining >= 0) {
                remainingOps.compareAndSet(current, remaining);
                return desiredOps;
            } else {
                remainingOps.compareAndSet(current, 0);
                return current;
            }
        }
        
        @Override
        public <T extends Document> boolean create(Collection<T> collection,
                List<UpdateOp> updateOps) {
            Lock lock = rwLock().writeLock();
            lock.lock();
            try {
                ConcurrentSkipListMap<String, T> map = getMap(collection);
                for (UpdateOp op : updateOps) {
                    if (map.containsKey(op.getId())) {
                        return false;
                    }
                }
                
                final List<UpdateOp> trimmedOps = new LinkedList<>();
                final List<UpdateOp> lateOps = new LinkedList<>();
                trimOps(collection, updateOps, trimmedOps, lateOps);
                boolean result = super.create(collection, trimmedOps);
                registerWrittenUpdateOp(collection, trimmedOps);
                if (trimmedOps.size() != updateOps.size()) {
                    breakpoint("wanted " + updateOps.size() + " ops, allowed only " + trimmedOps.size());
                    super.create(collection, lateOps);
                }
                return result;
            
            } finally {
                lock.unlock();
            }
        }

        private <T extends Document> void trimOps(Collection<T> collection, 
                List<UpdateOp> updateOps, List<UpdateOp> immediateOps, List<UpdateOp> lateOps) {
            int max = allowedOps(collection, updateOps.size());
            ArrayList<UpdateOp> al = new ArrayList<>(updateOps);
            immediateOps.addAll(al.subList(0, max));
            lateOps.addAll(al.subList(max, updateOps.size()));
        }
        
        private <T extends Document> List<String> trimString(Collection<T> collection, List<String> updateOps) {
            int max = allowedOps(collection, updateOps.size());
            ArrayList<String> al = new ArrayList<>(updateOps);
            List<String> trimmedUpdateOps = al.subList(0, max);
            return trimmedUpdateOps;
        }
        
        private ReadWriteLock rwLock() {
            try {
                return (ReadWriteLock) PrivateAccessor.getField(this, "rwLock");
            } catch (NoSuchFieldException e) {
                fail(e.getMessage());
                // never reached:
                return null;
            }
        }

        @Override
        public <T extends Document> @Nullable T createOrUpdate(Collection<T> collection,
                UpdateOp update) {
            allowOp(collection);
            registerWrittenUpdateOp(collection, update);
            return super.createOrUpdate(collection, update);
        }
        
        private <T extends Document> List<T> superCreateOrUpdate(Collection<T> collection, List<UpdateOp> updateOps) {
            List<T> result = new ArrayList<T>(updateOps.size());
            for (UpdateOp update : updateOps) {
                result.add(super.createOrUpdate(collection, update));
            }
            return result;
        }
        
        @Override
        public <T extends Document> List<T> createOrUpdate(Collection<T> collection,
                List<UpdateOp> updateOps) {
            final List<UpdateOp> trimmedOps = new LinkedList<>();
            final List<UpdateOp> lateOps = new LinkedList<>();
            trimOps(collection, updateOps, trimmedOps, lateOps);
            List<T> result = superCreateOrUpdate(collection, trimmedOps);
            registerWrittenUpdateOp(collection, trimmedOps);
            if (trimmedOps.size() != updateOps.size()) {
                breakpoint("wanted " + updateOps.size() + " ops, allowed only " + trimmedOps.size());
                superCreateOrUpdate(collection, lateOps);
            }
            return result;
        }
        
        @Override
        public <T extends Document> T findAndUpdate(Collection<T> collection,
                UpdateOp update) {
            allowOp(collection);
            registerWrittenUpdateOp(collection, update);
            return super.findAndUpdate(collection, update);
        }
        
        @Override
        public <T extends Document> void remove(Collection<T> collection,
                List<String> keys) {
            throw new IllegalStateException("not yet implemented");
//            super.remove(collection, trimString(collection, keys));
        }
        
        @Override
        public <T extends Document> int remove(Collection<T> collection,
                Map<String, Long> toRemove) {
            throw new IllegalStateException("not yet implemented");
        }
        
        @Override
        public <T extends Document> void remove(Collection<T> collection, String key) {
            allowOp(collection);
            super.remove(collection, key);
        }
        
        @Override
        public <T extends Document> int remove(Collection<T> collection,
                String indexedProperty, long startValue, long endValue)
                throws DocumentStoreException {
            throw new IllegalStateException("not yet implemented");
        }
    }

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Clock virtualClock;
    private DocumentNodeStore ns1;
    private DocumentNodeStore ns2;

    private ControllableMemoryDocumentStore store;
    
    @Before
    public void setup() throws Exception {
        int createOrUpdateBatchSize = 1;
        setup(createOrUpdateBatchSize);
    }

    private void setup(int createOrUpdateBatchSize) throws InterruptedException {
        System.setProperty("oak.documentMK.createOrUpdateBatchSize", String.valueOf(createOrUpdateBatchSize));
        virtualClock = new Clock.Virtual();
        virtualClock.waitUntil(System.currentTimeMillis());
        ClusterNodeInfo.setClock(virtualClock);
        store = new ControllableMemoryDocumentStore(Integer.MAX_VALUE, new BreakpointCallback() {
            
            @Override
            public void breakpointCallback(String contextMsg) {
                throw new Error("Crashing Now : " + contextMsg);
            }
        });
        ns1 = createDNS(store, 1);
        ns2 = createDNS(store, 2);
    }
    
    @After
    public void tearDown() throws Exception {
        store.getRemainingOps().set(Integer.MAX_VALUE);
        System.clearProperty("oak.documentMK.createOrUpdateBatchSize");
    }
    
    private void assertChildren(DocumentNodeStore ns, String... childPaths) {
        final DocumentNodeState root = ns.getRoot();
        for (String aChildPath : childPaths) {
            boolean inverted = false;
            if (aChildPath.startsWith("!")) {
                aChildPath = aChildPath.substring(1);
                inverted = true;
            }
            Path p = Path.fromString(aChildPath);
            NodeState node = root;
            for (String name : p.elements()) {
                node = node.getChildNode(name);
            }
            assertEquals(aChildPath + " exists :" + node.exists(), !inverted, node.exists());
        }
    }
    
    private void crashSafely(DocumentNodeStore ns) throws NoSuchFieldException {
        crashSafely(ns, true);
    }
    
    private void crashSafely(DocumentNodeStore ns, boolean disposeClusterInfo) throws NoSuchFieldException {
        if (disposeClusterInfo) {
            ns.getClusterInfo().dispose();
        }
        try {
            ns.dispose();
            fail("should have errored");
        } catch(Error e) {
            // expected
        }
        AtomicBoolean stopLeaseUpdateThread = (AtomicBoolean) PrivateAccessor.getField(ns, "stopLeaseUpdateThread");
        stopLeaseUpdateThread.set(true);
        synchronized (stopLeaseUpdateThread) {
            stopLeaseUpdateThread.notifyAll();
        }
        // trigger one last lease update in exactly this moment - otherwise it'll do it in an unpredictable future moment
        // but if no lease update was necessary, never mind
        ns.renewClusterIdLease();
    }
    
    /**
     * Illustrates creating an orphaned node (that can then cause the usual OakMerge0004) 
     * Steps:
     * 1. create a subtree partially, eg create /a, /a/b, /a/b/c
     * 2. then crash
     * 3. then lastRevRecovery
     * 4. then continue on the partial subtree adding further children, eg /a/b/c/d, /a/b/c/d/e
     * 5. then do all sorts of asserts on the resulting status
     */
    @Test
    public void testOrphanedNodes() throws Exception {
        System.setProperty("oak.documentMK.createOrUpdateBatchSize", "42");
        ns2.dispose();
        final DocumentNodeStore ns3 = createDNS(store, 3);
        store.setRestrictedCollections(new HashSet<>());
        final Semaphore breakpointReached = new Semaphore(0);
        final Semaphore breakpointContinue = new Semaphore(0);
        final Semaphore successSignal = new Semaphore(0);
        Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    createLeaves(ns3, "/a/b/c", "/a/b/c/d/e", "/a/b/c/d/e/f/g");
                    fail("should fail with a lease update failure");
                } catch(CommitFailedException ce) {
                    // it should actually fail due to sending the commitRoot update
                    successSignal.release(1);
                }
            }
        };
        final Thread lateWriterThread = new Thread(r);
        store.setBreakpointCallback(new BreakpointCallback() {
            
            @Override
            public void breakpointCallback(String contextMsg) {
                if (Thread.currentThread() != lateWriterThread) {
                    // only block Thread lateWriterThread
                    // other threads treat with an Error instead
                    throw new Error("crashing with msg : " + contextMsg);
                }
                breakpointReached.release(1);
                try {
                    while (!breakpointContinue.tryAcquire(1, 1000, TimeUnit.MILLISECONDS)) {
                        System.out.println("DocumentStore breakpoint still blocked");
                    }
                    System.out.println("done");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        
        assertTrue(ns3.getClusterInfo().toString().contains("state: ACTIVE"));
        advanceClockOneSecond();
        store.getRemainingOps().set(3);
        assertTrue(ns3.getClusterInfo().toString().contains("state: ACTIVE"));
        lateWriterThread.setDaemon(true);
        lateWriterThread.start();
        assertTrue("worker thread failed to reach breakpointReached in time", 
                breakpointReached.tryAcquire(1, 5000, TimeUnit.MILLISECONDS));
        assertTrue(ns3.getClusterInfo().toString().contains("state: ACTIVE"));
        advanceClock(11000);
//        crashSafely(ns2, false);
        assertTrue(ns3.getClusterInfo().toString().contains("state: ACTIVE"));
        store.getRemainingOps().set(Integer.MAX_VALUE);
        advanceClock(85000);
        ns1.runBackgroundOperations();
        ns1.renewClusterIdLease();
        advanceClock(65000);
        ns1.runBackgroundOperations();
        ns1.renewClusterIdLease();

        try {
            ns1.getLastRevRecoveryAgent().performRecoveryIfNeeded();
        } catch(Throwable th) {
            th.printStackTrace();
            fail("errr : " + th);
        }
        LeaseCheckDocumentStoreWrapper s = (LeaseCheckDocumentStoreWrapper) ns1.getDocumentStore();
        System.out.println(PrivateAccessor.getField(s, "delegate"));
        
        // now release the original thread from the lateWriterThread
        breakpointContinue.release(1);

        assertTrue("worker thread did not finish", successSignal.tryAcquire(1, 5000, TimeUnit.MILLISECONDS));

        advanceClockOneSecond();
        ns1.runBackgroundOperations();
        
        // now do those asserts, since now the data should be inconsistent
        {
            DocumentNodeState root = ns1.getRoot();
            assertFalse(root.hasChildNode("a"));
        }
        createLeaves(ns1, "/a");
        {
            DocumentNodeState root = ns1.getRoot();
            assertFalse(root.getChildNode("a").hasChildNode("b"));
        }
        createLeaves(ns1, "/a/b");
        {
            DocumentNodeState root = ns1.getRoot();
            assertFalse(root.getChildNode("a").getChildNode("b").hasChildNode("c"));
        }
        createLeaves(ns1, "/a/b/c");
        {
            DocumentNodeState root = ns1.getRoot();
            assertFalse(root.getChildNode("a").getChildNode("b").getChildNode("c").hasChildNode("d"));
            assertFalse(root.getChildNode("a").getChildNode("b").getChildNode("c").getChildNode("d").hasChildNode("e"));
        }
        try {
//        createLeaves(ns1, "/a/b/c/d");
            createLeaves(ns1, "/a/b/c/d/e");
            fail("should have failed");
        } catch(CommitFailedException cfe) {
            assertTrue(cfe.toString().contains("OakMerge0004"));
        }
//        ns1.getMBean().recover("/a/b/c/d", 3);
//        createLeaves(ns1, "/a/b/c/d/e");
    }
    
    private void advanceClock(long i) throws InterruptedException {
        virtualClock.waitUntil(virtualClock.getTime() + i);        
    }

    private class LeavesBuilder {
        
        private List<List<String>> transactions = new LinkedList<>();
        
        LeavesBuilder() {
            
        }
        
        LeavesBuilder add(String... leaves) {
            transactions.add(Arrays.asList(leaves));
            return this;
        }

        public void assertLeavesExist(DocumentNodeStore ns1) {
            List<String> allLeaves = new LinkedList<>();
            for (List<String> leaves : transactions) {
                allLeaves.addAll(leaves);
            }
            assertChildren(ns1, allLeaves.toArray(new String[0]));
        }
        
    }
    
    private void createContent(LeavesBuilder cb, DocumentNodeStore writingDns, DocumentNodeStore... dnss) throws CommitFailedException, InterruptedException {
        List<String> allLeaves = new LinkedList<>();
        for (List<String> leaves : cb.transactions) {
            createLeaves(writingDns, leaves.toArray(new String[0]));
            advanceClockOneSecond();
            allLeaves.addAll(leaves);
            assertChildren(writingDns, allLeaves.toArray(new String[0]));
        }
        advanceClockOneSecond();
        assertChildren(writingDns, allLeaves.toArray(new String[0]));
        advanceClockOneSecond();
        // now sync within cluster a couple times
        List<DocumentNodeStore> allDnss = new LinkedList<>(Arrays.asList(dnss));
        assertFalse(allDnss.contains(writingDns));
        allDnss.add(writingDns);
        runBackgroundOps(3, allDnss.toArray(new DocumentNodeStore[0]));
        advanceClockOneSecond();
    }

    interface ChangeBuilder {
        void modify(NodeBuilder nb);
    }

    void produceLateWriteSituation(int allowedUpdateOps, DocumentNodeStore dns, ChangeBuilder cb) throws NoSuchFieldException {
        store.getRemainingOps().set(allowedUpdateOps);
        store.clearOps();
        store.printOps();
        try {
            @NotNull
            NodeBuilder rb = dns.getRoot().builder();
            cb.modify(rb);
            dns.merge(rb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fail("should have failed");
        } catch(CommitFailedException ce) {
            // expected
        }
        crashSafely(dns);
        store.getRemainingOps().set(Integer.MAX_VALUE);
        System.out.println("These UpdateOps were still sent");
        store.printOps();
        System.out.println("These UpdateOps were still sent : DONE");        
    }
    
    private void removeNodes(NodeBuilder rb, String... paths) {
        for (String aPath : paths) {
            Path p = Path.fromString(aPath);
            NodeBuilder b = rb;
            for (String name : p.elements()) {
                b = b.child(name);        
            }
            b.remove();
        }
    }

    private void doTestAddNode(int createOrUpdateBatchSize, String targetTestNode)
            throws InterruptedException, CommitFailedException, NoSuchFieldException {
        setup(createOrUpdateBatchSize);
        
        // test setup, using ns1
        LeavesBuilder leaves = 
                new LeavesBuilder().add("/foo").add("/foo/a");
        createContent(leaves, ns1, ns2);

        // now produce JVM-pause/lateWrite/crash situation, using ns2
        produceLateWriteSituation(2, ns2, (rb) -> createLeaves(rb, "/foo/a/b/c/d", "/foo/e/f/g/h"));

        // ensure the above didn't manage to add nodes
        advanceClockOneSecond();
        leaves.assertLeavesExist(ns1);
        
        // now cause a conflict due to the late write
        try {
            setProperty(2, targetTestNode, "p2", "v2");
            fail("should fail");
        } catch(CommitFailedException cfe) {
            assertTrue(cfe.toString().contains("OakMerge0004"));
        }
    }

    /**
     * Illustrates a late-write add-node can later on cause an OakMerge0004 conflict
     */
    @Test
    public void testAddNode_variantA() throws Exception {
        doTestAddNode(1, "/foo/a/b/c/d");
    }

    /**
     * Illustrates a late-write add-node can later on cause an OakMerge0004 conflict
     */
    @Test
    public void testAddNode_variantB() throws Exception {
        doTestAddNode(1, "/foo/a/b");
    }

    private void doTestRemoveNode(int createOrUpdateBatchSize, String targetTestNode) throws Exception {
        setup(createOrUpdateBatchSize);
        
        // test setup, using ns1
        LeavesBuilder leaves = 
                new LeavesBuilder().add("/foo").add("/foo/a/b/c/d", "/foo/e/f/g/h", "/foo/e/f/i/j");
        createContent(leaves, ns1, ns2);

        // now produce JVM-pause/lateWrite/crash situation, using ns2
        produceLateWriteSituation(2, ns2, (rb) -> removeNodes(rb, "/foo/a/b/c/d", "/foo/e/f/g/h"));

        // ensure the above didn't manage to delete properly
        advanceClockOneSecond();
        leaves.assertLeavesExist(ns1);
        
        // now cause a conflict due to the late write
        try {
            setProperty(2, targetTestNode, "p2", "v2");
            fail("should fail");
        } catch(CommitFailedException cfe) {
            assertTrue(cfe.toString().contains("OakMerge0004"));
        }
    }
    
    /**
     * Illustrates a late-write remove-node can later on cause an OakMerge0004 conflict
     */
    @Test
    public void testRemoveNode_variantA() throws Exception {
        doTestRemoveNode(1, "/foo/a/b/c/d");
    }

    /**
     * Illustrates a late-write remove-node can later on cause an OakMerge0004 conflict
     */
    @Test
    public void testRemoveNode_variantB() throws Exception {
        doTestRemoveNode(1, "/foo/e/f/g/h");
    }

    /**
     * Illustrates a late-write remove-node can later on cause an OakMerge0004 conflict
     */
    @Test
    public void testRemoveNode_variantC() throws Exception {
        doTestRemoveNode(10, "/foo/e/f/g/h");
    }
    
    private void runBackgroundOps(int times, DocumentNodeStore... dnss) {
        for( int i = 0; i < times; i++) {
            for (DocumentNodeStore dns : dnss) {
                dns.runBackgroundOperations();
            }
        }
    }
    
    private void advanceClock(long duration, TimeUnit unit) throws InterruptedException {
        advanceClock(unit.toMillis(duration));
    }

    private void advanceClockOneSecond() throws InterruptedException {
        advanceClock(1, TimeUnit.SECONDS);
    }
    
    private void printNodes(DocumentNodeStore ns, String... paths) {
        for (String aPath : paths) {
            Path p = Path.fromString(aPath);
            NodeState n = ns.getRoot();
            for (String name : p.elements()) {
                n = n.getChildNode(name);
            }
            System.out.println(aPath + " :");
            if (!n.exists()) {
                System.out.println("DOES NOT EXIST");
            } else {
                DocumentNodeState dns = (DocumentNodeState) n;
                NodeDocument doc = dns.getDocument();
                System.out.println(doc.format());
            }
            System.out.println();
        }
    }

    /**
     * Illustrates a late-write change-property does *NOT* cause a later conflict
     */
    @Test
    public void changeUncommittedProperty() throws Exception {
        assertFalse(ns1.getRoot().hasChildNode("a"));

        @NotNull
        NodeBuilder rb = ns1.getRoot().builder();
        rb.child("a");
        ns1.merge(rb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(ns1.getRoot().hasChildNode("a"));
        
        createLeaves(ns1, "/foo/bar", "/foo/baz");
        setStringProperty(ns1, "/foo/bar", "a", "1");
        assertEquals("1", getStringProperty(ns1, "/foo/bar", "a"));
        assertEquals("true", getStringProperty(ns1, "/foo/bar", "leaf"));
        assertNull(getStringProperty(ns1, "/foo/bar", "crashleaf"));
        ns1.runBackgroundOperations();
        createChangeAfterRecovery(3, 2, "/foo/bar", "/foo/baz");
        store.getRemainingOps().set(Integer.MAX_VALUE);
        assertNull(getStringProperty(ns1, "/foo/bar", "crashleaf"));
        setStringProperty(ns1, "/foo/bar", "crashleaf", "false");
    }
    
    private String getStringProperty(DocumentNodeStore ns, String path, String key) {
        DocumentNodeState r = ns.getRoot();
        Path p = Path.fromString(path);
        NodeState s = r;
        for (String name : p.elements()) {
            s = s.getChildNode(name);
        }
        return s.getString(key);
    }

    private void setStringProperty(DocumentNodeStore ns, String path, String key,
            String value) throws CommitFailedException {
        NodeBuilder rb = ns.getRoot().builder();
        Path p = Path.fromString(path);
        NodeBuilder b = rb;
        for (String name : p.elements()) {
            b = b.child(name);
        }
        b.setProperty(key, value);
        ns.merge(rb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private void createLeaves(DocumentNodeStore ns, String... paths) throws CommitFailedException {
        @NotNull
        NodeBuilder rb = ns.getRoot().builder();
        createLeaves(rb, paths);
        
        ns.merge(rb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private void createLeaves(NodeBuilder rb, String... paths) {
        for (String aPath : paths) {
            Path p = Path.fromString(aPath);
            NodeBuilder b = rb;
            for (String name : p.elements()) {
                b = b.child(name);
            }
            b.setProperty("leaf", "true");
        }
    }

    private void setProperty(int clusterId, String path, String key, String value) throws CommitFailedException {
        DocumentNodeStore ns = createDNS(store, clusterId);
        ns.runBackgroundOperations();
        
        setProperty(ns, path, key, value);
        ns.runBackgroundOperations();
        ns.dispose();
    }

    private void setProperty(DocumentNodeStore ns, String path, String key,
            String value) throws CommitFailedException {
        NodeBuilder rb = ns.getRoot().builder();
        Path p = Path.fromString(path);
        NodeBuilder b = rb;
        for (String name : p.elements()) {
            b = b.child(name);
        }
        b.setProperty(key, value);
        
        ns.merge(rb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
    
    /**
     * This creates a change (revisions) after an instance has 
     * crashed and has been recovered by another instance.
     * The change is done in a few different subtrees and has
     * not been fully committed yet (the commit value on the 
     * commit root has not been set yet)
     * @throws CommitFailedException 
     */
    private void createChangeAfterRecovery(int clusterId, int allowedOps,
            String... paths) throws CommitFailedException {
        DocumentNodeStore ns = createDNS(store, clusterId);
        
        setStringProperty(ns, "/", "dummyPropForSweeper", "true");
        ns.runBackgroundOperations();
        
        @NotNull
        NodeBuilder rb = ns.getRoot().builder();
        for (String aPath : paths) {
            Path p = Path.fromString(aPath);
            NodeBuilder b = rb;
            for (String name : p.elements()) {
                b = b.child(name);
            }
            b.setProperty("crashleaf", "true");
        }
        
        store.getRemainingOps().set(allowedOps);
        try {
            ns.merge(rb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fail("did not fail");
        } catch(CommitFailedException ce) {
            // expected
        }
    }
    
    private DocumentNodeStore createDNS(DocumentStore ds, int clusterId){
        return create(ds, clusterId).getNodeStore();
    }

    private DocumentMK create(DocumentStore ds, int clusterId){
        return builderProvider.newBuilder().clock(virtualClock).setAsyncDelay(0)
                .setDocumentStore(ds).setClusterId(clusterId).open();
    }
}
