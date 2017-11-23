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

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
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
        Commit c = ns.newCommit(ns.getHeadRevision(), null);
        try {
            c.addNode(new DocumentNodeState(ns, "/foo/baz",
                    new RevisionVector(c.getRevision())));
            UpdateOp op = c.getUpdateOperationForNode("/bar");
            op.setMapEntry("p", c.getRevision(), "v");
            try {
                c.apply();
                ns.done(c, false, CommitInfo.EMPTY);
            } catch (DocumentStoreException e) {
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
        Commit c = ns.newCommit(ns.getHeadRevision(), null);
        try {
            c.addNode(new DocumentNodeState(ns, "/foo",
                    new RevisionVector(c.getRevision())));
            try {
                c.apply();
                ns.done(c, false, CommitInfo.EMPTY);
                fail("commit must fail");
            } catch (DocumentStoreException e) {
                // expected
                assertTrue("Unexpected exception message: " + e.getMessage(),
                        e.getMessage().contains("older than base"));
            }
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
        Commit c = ns.newCommit(ns.getHeadRevision().asBranchRevision(ns.getClusterId()), null);
        try {
            c.removeNode("/foo", EMPTY_NODE);
            try {
                c.apply();
                fail("commit must fail");
            } catch (DocumentStoreException e) {
                // expected
                assertTrue("Unexpected exception message: " + e.getMessage(),
                        e.getMessage().contains("does not exist"));
            }
        } finally {
            ns.canceled(c);
        }
    }
}
