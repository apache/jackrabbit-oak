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
package org.apache.jackrabbit.oak.plugins.cow;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class COWNodeStoreTest {

    private NodeStore store;

    private COWNodeStore cowNodeStore;

    @Before
    public void createCowNodeStore() {
        store = new MemoryNodeStore();
        cowNodeStore = new COWNodeStore(store);
    }

    @Test
    public void changesInCowMode() throws CommitFailedException {
        NodeState root = cowNodeStore.getRoot();
        NodeBuilder builder = root.builder();
        builder.child("abc");
        cowNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        cowNodeStore.enableCopyOnWrite();

        root = cowNodeStore.getRoot();
        builder = root.builder();
        builder.child("foo");
        cowNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        root = store.getRoot();
        builder = root.builder();
        builder.child("bar");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue("Change introduced before enabling the CoW mode is not available", cowNodeStore.getRoot().hasChildNode("abc"));
        assertTrue("Change introduced after enabling the CoW mode is not available", cowNodeStore.getRoot().hasChildNode("foo"));
        assertFalse("Changed introduced to the main store after enabling the CoW mode shouldn't be visible", cowNodeStore.getRoot().hasChildNode("bar"));

        assertTrue("Change introduced before enabling the CoW mode should be visible is the main store", store.getRoot().hasChildNode("abc"));
        assertFalse("Change introduced after enabling the CoW mode shouldn't be visible in the main store", store.getRoot().hasChildNode("foo"));
        assertTrue("Change introduced to the main store should be visible", store.getRoot().hasChildNode("bar"));

        cowNodeStore.disableCopyOnWrite();

        assertTrue("Change introduced before enabling the CoW mode is not available", cowNodeStore.getRoot().hasChildNode("abc"));
        assertFalse("Change introduced in the CoW mode should be dropped after disabling it", cowNodeStore.getRoot().hasChildNode("foo"));
        assertTrue("Change introduced to the main store should be visible", cowNodeStore.getRoot().hasChildNode("bar"));
    }

    @Test
    public void checkpointsInCowMode() throws CommitFailedException {
        String checkpoint1 = cowNodeStore.checkpoint(Long.MAX_VALUE, of("k", "v"));
        cowNodeStore.enableCopyOnWrite();

        Map<String, String> info = cowNodeStore.checkpointInfo(checkpoint1);
        assertEquals("The checkpoint is not inherited", of("k", "v"), info);

        NodeState root = cowNodeStore.getRoot();
        NodeBuilder builder = root.builder();
        builder.child("foo");
        cowNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        String checkpoint2 = cowNodeStore.checkpoint(Long.MAX_VALUE, of("k", "v2"));
        info = cowNodeStore.checkpointInfo(checkpoint2);
        assertEquals("The new checkpoint is not available", of("k", "v2"), info);
        assertTrue("The retrieve() doesn't work for the new checkpoint", cowNodeStore.retrieve(checkpoint2).hasChildNode("foo"));
        assertEquals(ImmutableList.of(checkpoint1, checkpoint2), cowNodeStore.checkpoints());

        assertTrue("The new checkpoint shouldn't be stored in the main store", store.checkpointInfo(checkpoint2).isEmpty());

        cowNodeStore.disableCopyOnWrite();
        assertTrue("The new checkpoint should be dropped after disabling the CoW mode", cowNodeStore.checkpointInfo(checkpoint2).isEmpty());
    }
}
