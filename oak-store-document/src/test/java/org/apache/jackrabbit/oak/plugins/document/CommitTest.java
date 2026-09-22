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

package org.apache.jackrabbit.oak.plugins.document;

import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.persistToBranch;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommitTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void testModifiedTime(){
        assertEquals(10, NodeDocument.getModifiedInSecs(10000));
        assertEquals(10, NodeDocument.getModifiedInSecs(10003));
        assertEquals(10, NodeDocument.getModifiedInSecs(12000));
        assertEquals(15, NodeDocument.getModifiedInSecs(15000));
        assertEquals(15, NodeDocument.getModifiedInSecs(15006));
    }

    // OAK-3733
    @Test
    public void detectConflict() throws Exception {
        // prepare node store
        DocumentNodeStore ns = builderProvider.newBuilder().getNodeStore();
        NodeBuilder b = ns.getRoot().builder();
        b.child("foo");
        b.child("bar");
        ns.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        b = ns.getRoot().builder();
        b.child("foo").remove();
        ns.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // this commit should fail
        Commit c = ns.newCommit(changes -> {
            changes.addNode(Path.fromString("/foo/baz"));
        }, ns.getHeadRevision(), null);
        try {
            UpdateOp op = c.getUpdateOperationForNode(Path.fromString("/bar"));
            op.setMapEntry("p", c.getRevision(), "v");
            try {
                c.apply();
                ns.done(c, false, CommitInfo.EMPTY);
            } catch (ConflictException e) {
                // expected
            }
        } finally {
            ns.canceled(c);
        }

        // try to re-create /foo
        b = ns.getRoot().builder();
        b.child("foo");
        ns.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    // OAK-4321
    @Test
    public void mergeExceptionMessage() throws Exception {
        // prepare node store
        DocumentNodeStore ns = builderProvider.newBuilder().getNodeStore();
        NodeBuilder b = ns.getRoot().builder();
        b.child("foo");
        ns.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        b = ns.getRoot().builder();
        b.child("bar");
        ns.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // this commit should fail
        Commit c = ns.newCommit(changes -> {
            changes.addNode(Path.fromString("/foo"));
        }, ns.getHeadRevision(), null);
        try {
            c.apply();
            ns.done(c, false, CommitInfo.EMPTY);
            fail("commit must fail");
        } catch (ConflictException e) {
            // expected
            assertTrue("Unexpected exception message: " + e.getMessage(),
                    e.getMessage().contains("older than base"));
        } finally {
            ns.canceled(c);
        }
    }

    // OAK-4894
    @Test
    public void branchCommitFails() throws Exception {
        // prepare node store
        DocumentNodeStore ns = builderProvider.newBuilder().getNodeStore();

        // this branch commit must fail with a DocumentStoreException
        Commit c = ns.newCommit(changes -> {
            changes.removeNode(Path.fromString("/foo"), EMPTY_NODE);
        }, ns.getHeadRevision().asBranchRevision(ns.getClusterId()), null);
        try {
            try {
                c.apply();
                fail("commit must fail");
            } catch (ConflictException e) {
                // expected
                assertTrue("Unexpected exception message: " + e.getMessage(),
                        e.getMessage().contains("does not exist"));
            }
        } finally {
            ns.canceled(c);
        }
    }

    // OAK-8585
    @Test
    public void alreadyDeletedMessage() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder().getNodeStore();

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);
        builder = ns.getRoot().builder();
        builder.child("foo").remove();
        merge(ns, builder);

        Commit c = ns.newCommit(changes -> {
            changes.removeNode(Path.fromString("/foo"), EMPTY_NODE);
        }, ns.getHeadRevision().asBranchRevision(ns.getClusterId()), null);
        try {
            try {
                c.apply();
                fail("commit must fail");
            } catch (ConflictException e) {
                // expected
                assertThat(e.getMessage(), containsString("base revision"));
                assertThat(e.getMessage(), containsString("branch"));
            }
        } finally {
            ns.canceled(c);
        }
    }

    // OAK-12316
    @Test
    public void childOrderCleanupAlwaysEnabled() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder().getNodeStore();

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);

        // set :childOrder twice within the same local branch, persisting each
        // change as its own branch commit; the first branch commit's
        // :childOrder entry is only removed once the second branch commit
        // applies
        NodeBuilder branchBuilder = ns.getRoot().builder();
        branchBuilder.child("foo").setProperty(":childOrder", List.of("a", "b"), Type.NAMES);
        persistToBranch(branchBuilder);
        branchBuilder.child("foo").setProperty(":childOrder", List.of("b", "a"), Type.NAMES);
        persistToBranch(branchBuilder);

        merge(ns, branchBuilder);

        // there is no more FT_NOCOCLEANUP_OAK-10660 toggle to disable this
        // cleanup, so the first branch commit's :childOrder entry must have
        // been removed by the second, leaving only the latest revision
        NodeDocument doc = ns.getDocumentStore().find(Collection.NODES,
                Utils.getIdFromPath("/foo"), 0);
        assertNotNull(doc);
        Map<Revision, String> childOrderRevisions = doc.getLocalMap(":childOrder");
        assertEquals("Only the latest :childOrder revision must remain",
                1, childOrderRevisions.size());
    }
}
