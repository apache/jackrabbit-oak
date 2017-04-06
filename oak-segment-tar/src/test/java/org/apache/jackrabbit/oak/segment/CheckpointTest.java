/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Ignore;
import org.junit.Test;

public class CheckpointTest {

    @Test
    public void testCheckpoint() throws CommitFailedException, IOException {
        SegmentNodeStore store = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        addTestNode(store, "test-checkpoint");
        verifyNS(store, true);
        rmTestNode(store, "test-checkpoint");
        verifyNS(store, false);

        // gc?
        store.retrieve("missing-checkpoint");
    }

    @Test
    public void testRelease() throws CommitFailedException, IOException {
        SegmentNodeStore store = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        addTestNode(store, "test-checkpoint");
        String cp = verifyNS(store, true);

        store.release(cp);
        assertNull(store.retrieve(cp));

    }

    private static String verifyNS(SegmentNodeStore store, boolean exists) {
        String cp = store.checkpoint(TimeUnit.HOURS.toMillis(1));
        assertNotNull("Checkpoint must not be null", cp);

        NodeState cpns = store.retrieve(cp);
        assertNotNull(cpns);
        if (exists) {
            assertTrue("Node doesn't exist in checkpoint",
                    cpns.getChildNode("test-checkpoint").exists());
        } else {
            assertFalse("Node shouldn't exist in checkpoint", cpns
                    .getChildNode("test-checkpoint").exists());
        }
        return cp;
    }

    private static void addTestNode(NodeStore store, String name)
            throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child(name);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static void rmTestNode(NodeStore store, String name)
            throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child(name).remove();
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    /**
     * OAK-3587 test simulates a timeout while trying to create a checkpoint,
     * then releases the lock and tries again
     */
    @Test
    @Ignore("OAK-4122")
    public void testShortWait() throws Exception {
        final SegmentNodeStore store = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        
        // FIXME OAK-4122
        // store.setCheckpointsLockWaitTime(1);

        final Semaphore semaphore = new Semaphore(0);
        final AtomicBoolean blocking = new AtomicBoolean(true);

        final Callable<Boolean> block = new Callable<Boolean>() {

            @Override
            public Boolean call() {
                while (blocking.get()) {
                    if (semaphore.availablePermits() == 0) {
                        semaphore.release();
                    }
                }
                return true;
            }
        };

        Thread background = new Thread() {
            @Override
            public void run() {
                try {
                    // FIXME OAK-4122
                    // store.locked(block, 10, SECONDS);
                } catch (Exception e) {
                    //
                }
            }
        };

        background.start();
        semaphore.acquire();

        String cp0 = store.checkpoint(10);
        assertNull(store.retrieve(cp0));

        blocking.set(false);
        String cp1 = store.checkpoint(10);
        assertNotNull(store.retrieve(cp1));
    }

    /**
     * OAK-3587 test simulates a wait less than configured
     * {@code SegmentNodeStore#setCheckpointsLockWaitTime(int)} value so the
     * checkpoint call must return a valid value
     */
    @Test
    @Ignore("OAK-4122")
    public void testLongWait() throws Exception {
        final int blockTime = 1;
        final SegmentNodeStore store = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        
        // FIXME OAK-4122
        // store.setCheckpointsLockWaitTime(blockTime + 1);

        final Semaphore semaphore = new Semaphore(0);

        final Callable<Boolean> block = new Callable<Boolean>() {

            @Override
            public Boolean call() {
                try {
                    semaphore.release();
                    SECONDS.sleep(blockTime);
                } catch (InterruptedException e) {
                    //
                }
                return true;
            }
        };

        Thread background = new Thread() {
            @Override
            public void run() {
                try {
                    // FIXME OAK-4122
                    // store.locked(block, 10, SECONDS);
                } catch (Exception e) {
                    //
                }
            }
        };

        background.start();
        semaphore.acquire();

        String cp0 = store.checkpoint(10);
        assertNotNull(store.retrieve(cp0));
    }

    @Test
    public void testCheckpointMax() throws CommitFailedException, IOException {
        SegmentNodeStore store = SegmentNodeStoreBuilders.builder(
                new MemoryStore()).build();
        String cp0 = store.checkpoint(Long.MAX_VALUE);
        String cp1 = store.checkpoint(Long.MAX_VALUE);
        assertNotNull(store.retrieve(cp0));
        assertNotNull(store.retrieve(cp1));
    }
}
