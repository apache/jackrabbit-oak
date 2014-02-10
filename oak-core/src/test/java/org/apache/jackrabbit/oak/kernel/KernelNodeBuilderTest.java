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

package org.apache.jackrabbit.oak.kernel;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

public class KernelNodeBuilderTest {

    @Test
    public void deletesKernelNodeStore() throws CommitFailedException {
        NodeStore store = new KernelNodeStore(new MicroKernelImpl());
        init(store);
        run(store);
    }

    @Test
    public void deletesMemoryNodeStore() throws CommitFailedException {
        NodeStore store = new MemoryNodeStore();
        init(store);
        run(store);
    }

    @Test
    public void rebasePreservesNew() {
        NodeStore store = new KernelNodeStore(new MicroKernelImpl());
        NodeBuilder root = store.getRoot().builder();
        NodeBuilder added = root.setChildNode("added");
        assertTrue(root.hasChildNode("added"));
        assertTrue(added.isNew());
        store.rebase(root);
        assertTrue(added.exists());
        assertTrue(root.hasChildNode("added"));
        assertTrue(added.isNew());
    }

    @Test
    public void rebaseInvariant() {
        NodeStore store = new KernelNodeStore(new MicroKernelImpl());
        NodeBuilder root = store.getRoot().builder();
        NodeBuilder added = root.setChildNode("added");
        NodeState base = root.getBaseState();
        store.rebase(root);
        assertEquals(base, root.getBaseState());
    }

    @Test
    public void rebase() throws CommitFailedException {
        NodeStore store = new KernelNodeStore(new MicroKernelImpl());
        NodeBuilder root = store.getRoot().builder();
        modify(store);
        store.rebase(root);
        assertEquals(store.getRoot(), root.getBaseState());
    }

    private static void modify(NodeStore store) throws CommitFailedException {
        NodeBuilder root = store.getRoot().builder();
        root.setChildNode("added");
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static void init(NodeStore store) throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("x").child("y").child("z");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static void run(NodeStore store) throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();

        assertTrue("child node x should be present", builder.hasChildNode("x"));
        assertTrue("child node x/y should be present", builder.child("x")
                .hasChildNode("y"));
        assertTrue("child node x/y/z should be present", builder.child("x")
                .child("y").hasChildNode("z"));

        builder.getChildNode("x").remove();
        assertFalse("child node x not should be present",
                builder.hasChildNode("x"));
        assertFalse("child node x/y not should be present", builder.child("x")
                .hasChildNode("y"));

        // See OAK-531
        assertFalse("child node x/y/z not should not be present", builder
                .child("x").child("y").hasChildNode("z"));

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

}
