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
package org.apache.jackrabbit.oak.plugins.segment;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.junit.Test;

public class MergeTest {

    @Test
    public void testSequentialMerge() throws CommitFailedException {
        NodeStore store = new SegmentNodeStore(new MemoryStore());

        assertFalse(store.getRoot().hasProperty("foo"));
        assertFalse(store.getRoot().hasProperty("bar"));

        NodeStoreBranch a = store.branch();
        a.setRoot(a.getHead().builder().setProperty("foo", "abc").getNodeState());
        a.merge(EmptyHook.INSTANCE);

        assertTrue(store.getRoot().hasProperty("foo"));
        assertFalse(store.getRoot().hasProperty("bar"));

        NodeStoreBranch b = store.branch();
        b.setRoot(b.getHead().builder().setProperty("bar", "xyz").getNodeState());
        b.merge(EmptyHook.INSTANCE);

        assertTrue(store.getRoot().hasProperty("foo"));
        assertTrue(store.getRoot().hasProperty("bar"));
    }

    @Test
    public void testOptimisticMerge() throws CommitFailedException {
        NodeStore store = new SegmentNodeStore(new MemoryStore());

        NodeStoreBranch a = store.branch();
        a.setRoot(a.getHead().builder().setProperty("foo", "abc").getNodeState());

        NodeStoreBranch b = store.branch();
        b.setRoot(b.getHead().builder().setProperty("bar", "xyz").getNodeState());

        assertFalse(store.getRoot().hasProperty("foo"));
        assertFalse(store.getRoot().hasProperty("bar"));

        a.merge(EmptyHook.INSTANCE);

        assertTrue(store.getRoot().hasProperty("foo"));
        assertFalse(store.getRoot().hasProperty("bar"));

        b.merge(EmptyHook.INSTANCE);

        assertTrue(store.getRoot().hasProperty("foo"));
        assertTrue(store.getRoot().hasProperty("bar"));
    }

    @Test
    public void testPessimisticMerge() throws Exception {
        final SegmentNodeStore store = new SegmentNodeStore(new MemoryStore());
        final Semaphore semaphore = new Semaphore(0);
        final AtomicBoolean running = new AtomicBoolean(true);

        Thread background = new Thread() {
            @Override
            public void run() {
                for (int i = 0; running.get(); i++) {
                    try {
                        SegmentNodeStoreBranch a = store.branch();
                        NodeBuilder builder = a.getHead().builder();
                        builder.setProperty("foo", "abc" + i);
                        a.setRoot(builder.getNodeState());
                        a.merge(EmptyHook.INSTANCE);
                        semaphore.release();
                    } catch (CommitFailedException e) {
                        fail();
                    }
                }
            }
        };
        background.start();

        // wait for the first commit
        semaphore.acquire();

        assertTrue(store.getRoot().hasProperty("foo"));
        assertFalse(store.getRoot().hasProperty("bar"));

        SegmentNodeStoreBranch b = store.branch();
        b.setMaximumBackoff(100);
        b.setRoot(b.getHead().builder().setProperty("bar", "xyz").getNodeState());
        b.merge(new CommitHook() {
            @Override @Nonnull
            public NodeState processCommit(NodeState before, NodeState after) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    fail();
                }
                return after;
            }
        });

        assertTrue(store.getRoot().hasProperty("foo"));
        assertTrue(store.getRoot().hasProperty("bar"));

        running.set(false);
        background.join();
    }

}
