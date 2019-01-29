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

import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.asDocumentState;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.persistToBranch;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getRootDocument;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    @Ignore("OAK-8008")
    @Test
    public void orphanedBranchTest() {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(store).build();
        NodeBuilder builder = ns.getRoot().builder();
        builder.setProperty("p", "v");
        persistToBranch(builder);
        ns.dispose();

        // start again
        ns = builderProvider.newBuilder()
                .setDocumentStore(store).build();
        assertFalse(ns.getRoot().hasProperty("p"));
    }

    @Test
    public void compareBranchStates() {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0).build();
        ns.runBackgroundOperations();
        DocumentStore store = ns.getDocumentStore();

        NodeBuilder builder = ns.getRoot().builder();
        builder.setProperty("p", "a");
        persistToBranch(builder);
        NodeState s1 = builder.getNodeState();
        builder.setProperty("p", "b");
        persistToBranch(builder);
        NodeState s2 = builder.getNodeState();

        Set<String> changes = new HashSet<>();
        NodeStateDiff diff = new DefaultNodeStateDiff() {
            @Override
            public boolean propertyChanged(PropertyState before,
                                           PropertyState after) {
                changes.add(before.getName());
                return super.propertyChanged(before, after);
            }
        };
        s2.compareAgainstBaseState(s1, diff);
        assertThat(changes, contains("p"));

        NodeDocument root = getRootDocument(store);
        RevisionVector br = asDocumentState(s1).getRootRevision();
        assertTrue(br.isBranch());
        DocumentNodeState state = root.getNodeAtRevision(ns, br, null);
        assertNotNull(state);
        assertEquals("a", state.getString("p"));
    }
}
