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

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Ignore;
import org.junit.Test;

public class MergeTest {

    @Test
    public void testSequentialMerge() throws CommitFailedException, IOException {
        NodeStore store = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();

        assertFalse(store.getRoot().hasProperty("foo"));
        assertFalse(store.getRoot().hasProperty("bar"));

        NodeBuilder a = store.getRoot().builder();
        a.setProperty("foo", "abc");
        store.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(store.getRoot().hasProperty("foo"));
        assertFalse(store.getRoot().hasProperty("bar"));

        NodeBuilder b = store.getRoot().builder();
        b.setProperty("bar", "xyz");
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(store.getRoot().hasProperty("foo"));
        assertTrue(store.getRoot().hasProperty("bar"));
    }

    @Test
    public void testOptimisticMerge() throws CommitFailedException, IOException {
        NodeStore store = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();

        NodeBuilder a = store.getRoot().builder();
        a.setProperty("foo", "abc");

        NodeBuilder b = store.getRoot().builder();
        b.setProperty("bar", "xyz");

        assertFalse(store.getRoot().hasProperty("foo"));
        assertFalse(store.getRoot().hasProperty("bar"));

        store.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(store.getRoot().hasProperty("foo"));
        assertFalse(store.getRoot().hasProperty("bar"));

        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(store.getRoot().hasProperty("foo"));
        assertTrue(store.getRoot().hasProperty("bar"));
    }

    @Test
    @Ignore("OAK-4122")
    public void testPessimisticMerge() throws Exception {
        final SegmentNodeStore store = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        final Semaphore semaphore = new Semaphore(0);
        final AtomicBoolean running = new AtomicBoolean(true);

        Thread background = new Thread() {
            @Override
            public void run() {
                for (int i = 0; running.get(); i++) {
                    try {
                        NodeBuilder a = store.getRoot().builder();
                        a.setProperty("foo", "abc" + i);
                        store.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
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

        NodeBuilder b = store.getRoot().builder();
        b.setProperty("bar", "xyz");
        
        // FIXME OAK-4122
        //  store.setMaximumBackoff(100);
        store.merge(b, new CommitHook() {
            @Override @Nonnull
            public NodeState processCommit(
                    NodeState before, NodeState after, CommitInfo info) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    fail();
                }
                return after;
            }
        }, CommitInfo.EMPTY);

        assertTrue(store.getRoot().hasProperty("foo"));
        assertTrue(store.getRoot().hasProperty("bar"));

        running.set(false);
        background.join();
    }

}
