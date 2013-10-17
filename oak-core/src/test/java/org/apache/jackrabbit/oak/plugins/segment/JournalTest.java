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

import static org.junit.Assert.assertEquals;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class JournalTest {

    private final SegmentStore store = new MemoryStore();

    private final SegmentNodeStore root = new SegmentNodeStore(store);

    private final SegmentNodeStore left = new SegmentNodeStore(store, "left");

    private final SegmentNodeStore right = new SegmentNodeStore(store, "right");

    @Test
    public void testChangesFromRoot() throws CommitFailedException {
        NodeState oldState = root.getRoot();
        assertEquals(oldState, left.getRoot());
        assertEquals(oldState, right.getRoot());

        NodeBuilder builder = oldState.builder();
        builder.setProperty("foo", "bar");
        NodeState newState = builder.getNodeState();

        root.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertEquals(newState, root.getRoot());
        assertEquals(oldState, left.getRoot());
        assertEquals(oldState, right.getRoot());

        store.getJournal("left").merge();
        assertEquals(newState, root.getRoot());
        assertEquals(newState, left.getRoot());
        assertEquals(oldState, right.getRoot());

        store.getJournal("right").merge();
        assertEquals(newState, root.getRoot());
        assertEquals(newState, left.getRoot());
        assertEquals(newState, right.getRoot());
    }

    @Test
    public void testChangesToRoot() throws CommitFailedException {
        NodeState oldState = root.getRoot();
        assertEquals(oldState, left.getRoot());
        assertEquals(oldState, right.getRoot());

        NodeBuilder builder = oldState.builder();
        builder.setProperty("foo", "bar");
        NodeState newState = builder.getNodeState();

        left.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertEquals(oldState, root.getRoot());
        assertEquals(newState, left.getRoot());
        assertEquals(oldState, right.getRoot());

        store.getJournal("left").merge();
        assertEquals(newState, root.getRoot());
        assertEquals(newState, left.getRoot());
        assertEquals(oldState, right.getRoot());

        store.getJournal("right").merge();
        assertEquals(newState, root.getRoot());
        assertEquals(newState, left.getRoot());
        assertEquals(newState, right.getRoot());
    }

    @Test
    public void testConcurrentChanges() throws CommitFailedException {
        NodeState oldState = root.getRoot();
        assertEquals(oldState, left.getRoot());
        assertEquals(oldState, right.getRoot());

        NodeBuilder leftBuilder = oldState.builder();
        leftBuilder.setProperty("foo", "bar");
        NodeState leftState = leftBuilder.getNodeState();

        left.merge(leftBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertEquals(oldState, root.getRoot());
        assertEquals(leftState, left.getRoot());
        assertEquals(oldState, right.getRoot());

        store.getJournal("left").merge();
        assertEquals(leftState, root.getRoot());
        assertEquals(leftState, left.getRoot());
        assertEquals(oldState, right.getRoot());

        NodeBuilder rightBuilder = oldState.builder();
        rightBuilder.setProperty("bar", "foo");

        right.merge(rightBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store.getJournal("right").merge();
        NodeState newState = root.getRoot();
        assertEquals("bar", newState.getProperty("foo").getValue(Type.STRING));
        assertEquals("foo", newState.getProperty("bar").getValue(Type.STRING));
        assertEquals(leftState, left.getRoot());
        assertEquals(newState, right.getRoot());

        store.getJournal("left").merge();
        assertEquals(newState, root.getRoot());
        assertEquals(newState, left.getRoot());
        assertEquals(newState, right.getRoot());
    }

}
