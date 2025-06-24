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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.jackrabbit.oak.api.CommitFailedException.MERGE;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.persistToBranch;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class DocumentNodeStoreBranchTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void branchedBranch() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder().getNodeStore();
        NodeBuilder b1 = ns.getRoot().builder();
        b1.child("a");
        persistToBranch(b1);
        NodeBuilder b2 = b1.getNodeState().builder();
        b1.child("b");
        persistToBranch(b1);

        b2.child("c");
        persistToBranch(b2);
        assertTrue(b2.hasChildNode("a"));
        assertFalse(b2.hasChildNode("b"));
        assertTrue(b2.hasChildNode("c"));

        // b1 must still see 'a' and 'b', but not 'c'
        assertTrue(b1.hasChildNode("a"));
        assertTrue(b1.hasChildNode("b"));
        assertFalse(b1.hasChildNode("c"));

        merge(ns, b1);

        assertTrue(ns.getRoot().getChildNode("a").exists());
        assertTrue(ns.getRoot().getChildNode("b").exists());
        assertFalse(ns.getRoot().getChildNode("c").exists());

        // b2 must not be able to merge
        try {
            merge(ns, b2);
            fail("Merge must fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    /**
     * Similar test as {@link #branchedBranch()} but without persistent branch.
     */
    @Test
    public void builderFromStateFromBuilder() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder().getNodeStore();
        NodeBuilder b1 = ns.getRoot().builder();
        b1.child("a");

        NodeBuilder b2 = b1.getNodeState().builder();
        b1.child("b");

        b2.child("c");
        assertTrue(b2.hasChildNode("a"));
        assertFalse(b2.hasChildNode("b"));
        assertTrue(b2.hasChildNode("c"));

        // b1 must still see 'a' and 'b', but not 'c'
        assertTrue(b1.hasChildNode("a"));
        assertTrue(b1.hasChildNode("b"));
        assertFalse(b1.hasChildNode("c"));

        merge(ns, b1);

        assertTrue(ns.getRoot().getChildNode("a").exists());
        assertTrue(ns.getRoot().getChildNode("b").exists());
        assertFalse(ns.getRoot().getChildNode("c").exists());

        // b2 must not be able to merge
        try {
            merge(ns, b2);
            fail("Merge must fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void noopChanges() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder().setUpdateLimit(10).getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a").setProperty("p", 1);
        merge(ns, builder);
        NodeState root = ns.getRoot();
        builder = root.builder();
        builder.child("b");
        for (int i = 0; i < ns.getUpdateLimit() * 2000; i++) {
            builder.child("a").setProperty("p", 1);
        }
        builder.getNodeState().compareAgainstBaseState(root, new JsopDiff());
    }

    @Test // OAK-11720
    public void mergeRetriesWithExclusiveLock() throws Exception {
        // avoidMergeLock = false -> should retry with exclusive lock
        boolean AVOID_MERGE_LOCK = false;
        DocumentMK.Builder mkBuilder = builderProvider.newBuilder();
        DocumentNodeStoreStatsCollector statsCollector = mock(DocumentNodeStoreStatsCollector.class);
        mkBuilder.setNodeStoreStatsCollector(statsCollector);
        DocumentNodeStore store = mkBuilder.getNodeStore();
        // Max back-off time for retries.
        // It will retry with a waiting time of 50ms, 100ms, 200ms and 400ms (4 attempts in total).
        store.setMaxBackOffMillis(500);

        // Best way to simulate a merge failure is to use a CommitHook that throws
        // an exception on the first 4 attempts and succeeds on the 5th attempt.
        AtomicInteger hookInvocations = new AtomicInteger();
        CommitHook hook = (before, after, info) -> {
            int count = hookInvocations.incrementAndGet();
            if (count <= 4) { // Force a merge failure for the first 4 attempts
                throw new CommitFailedException(MERGE, 1000 + count, "simulated failure");
            } else {
                // on the 5th attempt will succeed
                return after;
            }
        };

        // create a test node to be merged
        NodeBuilder builder = store.getRoot().builder();
        builder.child("testNode").setProperty("testProperty", "testValue");

        DocumentNodeStoreBranch branch = new DocumentNodeStoreBranch(store, store.getRoot(),
                new ReentrantReadWriteLock(), AVOID_MERGE_LOCK // avoidMergeLock set to false - must retry with exclusive lock
        );
        branch.setRoot(builder.getNodeState());

        // Initially the test node must not exist
        assertFalse(store.getRoot().hasChildNode("testNode"));
        NodeState result = branch.merge(hook, CommitInfo.EMPTY);
        assertNotNull(result);
        // Check the CommitHook was invoked 5 times (4 failures + 1 success)
        assertEquals("CommitHook must be invoked 5 times", 5, hookInvocations.get());
        // The test node must now exist after the successful merge
        assertTrue("Node must be present after successful merge", store.getRoot().hasChildNode("testNode"));
        assertTrue("Property must be set after successful merge", store.getRoot().getChildNode("testNode").hasProperty("testProperty"));

        // Verify that first 4 attempts failed with exclusive == false
        verify(statsCollector).failedMerge(anyInt(), anyLong(), anyLong(), eq(false));
        // Verify that the last attempt succeeded with exclusive == true
        verify(statsCollector).doneMerge(anyInt(), anyInt(), anyLong(), anyLong(), eq(true));
        // Verify that no attempt without exclusive lock failed
        verify(statsCollector, never()).doneMerge(anyInt(), anyInt(), anyLong(), anyLong(), eq(false));
    }

    @Test // OAK-11720
    public void mergeRetriesWithoutExclusiveLock() {
        // avoidMergeLock = true -> should not retry with exclusive lock and fail immediately
        boolean AVOID_MERGE_LOCK = true;
        DocumentMK.Builder mkBuilder = builderProvider.newBuilder();
        DocumentNodeStoreStatsCollector statsCollector = mock(DocumentNodeStoreStatsCollector.class);
        mkBuilder.setNodeStoreStatsCollector(statsCollector);
        DocumentNodeStore store = mkBuilder.getNodeStore();
        // Max back-off time for retries.
        // It will retry with a waiting time of 50ms, 100ms, 200ms and 400ms (4 attempts in total)
        store.setMaxBackOffMillis(500);

        AtomicInteger hookInvocations = new AtomicInteger();
        CommitHook hook = (before, after, info) -> {
            int count = hookInvocations.incrementAndGet();
            if (count <= 4) { // Force a merge failure for the first 4 attempts
                throw new CommitFailedException(MERGE, 1000 + count, "simulated failure");
            } else {
                // on the 5th attempt will succeed
                return after;
            }
        };

        // create a test node to be merged
        NodeBuilder builder = store.getRoot().builder();
        builder.child("testNode").setProperty("testProperty", "testValue");

        DocumentNodeStoreBranch branch = new DocumentNodeStoreBranch(store, store.getRoot(),
                new ReentrantReadWriteLock(), AVOID_MERGE_LOCK // avoidMergeLock set to true - must fail after retries without exclusive lock
        );
        branch.setRoot(builder.getNodeState());

        // Initially the test node must not exist
        assertFalse(store.getRoot().hasChildNode("testNode"));
        try {
            branch.merge(hook, CommitInfo.EMPTY);
            fail("Merge must fail with CommitFailedException after all the attempts without exclusive lock");
        } catch (CommitFailedException e) {
            assertEquals(MERGE, e.getType());
            assertEquals(1004, e.getCode());
        }

        // Check the CommitHook was invoked 4 times (4 failures)
        assertEquals("CommitHook must be invoked 4 times", 4, hookInvocations.get());
        // The test node must NOT exist after the successful merge
        assertFalse("Node must be present after successful merge", store.getRoot().hasChildNode("testNode"));

        // Verify that first 4 attempts failed with exclusive == false
        verify(statsCollector).failedMerge(anyInt(), anyLong(), anyLong(), eq(false));
        // Verify that no attempt failed with exclusive == true
        verify(statsCollector, never()).failedMerge(anyInt(), anyLong(), anyLong(), eq(true));
        // Verify that no merge attempt happened with exclusive == true
        verify(statsCollector, never()).failedMerge(anyInt(), anyLong(), anyLong(), eq(true));
        // Verify that the merge never succeeded (with any value of exclusive lock)
        verify(statsCollector, never()).doneMerge(anyInt(), anyInt(), anyLong(), anyLong(), anyBoolean());
    }
}
