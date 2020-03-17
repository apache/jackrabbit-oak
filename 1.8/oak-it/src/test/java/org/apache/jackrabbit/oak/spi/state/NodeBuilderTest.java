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

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.junit.Test;

public class NodeBuilderTest extends OakBaseTest {

    public NodeBuilderTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Test
    public void deletes() throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("x").child("y").child("z");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        builder = store.getRoot().builder();
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

    @Test
    public void rebasePreservesNew() {
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
        NodeBuilder root = store.getRoot().builder();
        NodeBuilder added = root.setChildNode("added");
        NodeState base = root.getBaseState();
        store.rebase(root);
        assertEquals(base, root.getBaseState());
    }

    @Test
    public void rebase() throws CommitFailedException {
        NodeBuilder root = store.getRoot().builder();
        modify(store);
        store.rebase(root);
        assertEquals(store.getRoot(), root.getBaseState());
    }

    @Test
    public void isReplacedBehaviour() throws Exception{
        NodeBuilder nb = store.getRoot().builder();
        nb.child("a").setProperty("foo", "bar");

        store.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        nb = store.getRoot().builder();
        nb.child("a").child("b");
        assertFalse(nb.getChildNode("a").isReplaced("foo"));
    }

    private static void modify(NodeStore store) throws CommitFailedException {
        NodeBuilder root = store.getRoot().builder();
        root.setChildNode("added");
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

}
