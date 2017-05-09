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

package org.apache.jackrabbit.oak.spi.state;

import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;

import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CheckpointTest extends OakBaseTest {
    private NodeState root;

    public CheckpointTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setUp() throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder test = builder.child("test");
        test.setProperty("a", 1);
        test.setProperty("b", 2);
        test.setProperty("c", 3);
        test.child("x");
        test.child("y");
        test.child("z");
        root = store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @After
    public void tearDown() throws Exception {
        fixture.dispose(store);
    }

    @Test
    public void checkpoint() throws CommitFailedException {
        String cp = store.checkpoint(Long.MAX_VALUE);

        NodeBuilder builder = store.getRoot().builder();
        builder.setChildNode("new");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse(root.equals(store.getRoot()));
        assertEquals(root, store.retrieve(cp));

        assertTrue(store.release(cp));
        assertNull(store.retrieve(cp));
    }

    @Test
    public void checkpointInfo() throws CommitFailedException {
        ImmutableMap<String, String> props = ImmutableMap.of(
                "one", "1", "two", "2", "three", "2");
        String cp = store.checkpoint(Long.MAX_VALUE, props);
        assertEquals(props, store.checkpointInfo(cp));
    }

    @Test
    public void noContentChangeForCheckpoints() throws Exception{
        final AtomicInteger invocationCount = new AtomicInteger();
        ((Observable)store).addObserver(new Observer() {
            @Override
            public void contentChanged(@Nonnull NodeState root, @Nonnull CommitInfo info) {
                invocationCount.incrementAndGet();
            }
        });

        invocationCount.set(0);

        String cp = store.checkpoint(Long.MAX_VALUE);
        assertEquals(0, invocationCount.get());

        store.release(cp);
        assertEquals(0, invocationCount.get());

    }

    @Test
    public void retrieveAny() {
        assertTrue(store.retrieve("r42-0-0") == null);
    }

}
