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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.Branch.BranchCommit;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.asDocumentState;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.persistToBranch;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getRootDocument;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BranchTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void getModifiedPathsUntil() {
        UnmergedBranches branches = new UnmergedBranches();

        RevisionVector base = new RevisionVector(Revision.newRevision(1));
        Revision c1 = Revision.newRevision(1).asBranchRevision();
        Branch b = branches.create(base, c1, null);

        BranchCommit bc1 = b.getCommit(c1);
        bc1.track(Path.fromString("/foo"));

        Revision c2 = Revision.newRevision(1).asBranchRevision();
        b.addCommit(c2);
        BranchCommit bc2 = b.getCommit(c2);
        bc2.track(Path.fromString("/bar"));

        Revision c3 = Revision.newRevision(1).asBranchRevision();
        b.rebase(c3, new RevisionVector(Revision.newRevision(1)));

        Revision c4 = Revision.newRevision(1).asBranchRevision();
        b.addCommit(c4);
        BranchCommit bc4 = b.getCommit(c4);
        bc4.track(Path.fromString("/baz"));

        Revision c5 = Revision.newRevision(1).asBranchRevision();

        try {
            b.getModifiedPathsUntil(Revision.newRevision(1));
            fail("Must fail with IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }

        assertModifiedPaths(b.getModifiedPathsUntil(c1), "/foo");
        assertModifiedPaths(b.getModifiedPathsUntil(c2), "/foo", "/bar");
        assertModifiedPaths(b.getModifiedPathsUntil(c3), "/foo", "/bar");
        assertModifiedPaths(b.getModifiedPathsUntil(c4), "/foo", "/bar", "/baz");
        assertModifiedPaths(b.getModifiedPathsUntil(c5));
    }

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

    private void assertModifiedPaths(Iterable<Path> actual, String... expected) {
        Set<Path> expectedSet = Sets.newHashSet();
        for (String p : expected) {
            expectedSet.add(Path.fromString(p));
        }
        assertEquals(expectedSet, Sets.newHashSet(actual));
    }
}
